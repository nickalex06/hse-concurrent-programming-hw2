package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {

    private final ConnectionFactory factory;
    private final ReentrantLock lock;
    private final Condition idleChanged;
    private final Condition stateChanged;
    private final ArrayDeque<IdleConnection> idle = new ArrayDeque<>();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final Set<Lease> borrowed = new HashSet<>();
    private final Thread cleaner;
    private final long idleTimeoutNanos;
    private PoolConfig config;
    private int creating;
    private int closing;
    private boolean stopped;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "Настройки пула не заданы");
        this.factory = Objects.requireNonNull(factory, "Фабрика соединений не задана");
        lock = new ReentrantLock(config.fair());
        idleChanged = lock.newCondition();
        stateChanged = lock.newCondition();
        idleTimeoutNanos = nanos(config.idleTimeout());
        cleaner = new Thread(this::cleanIdle, "connection-pool-cleaner");
        cleaner.setDaemon(true);
        cleaner.start();
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        long budget = checkedNanos(timeout);
        long started = System.nanoTime();
        List<RawConnection> toClose = new ArrayList<>();
        lock.lockInterruptibly();
        try {
            checkRunning();
            if (waiters.isEmpty() && !idle.isEmpty()) {
                return lease(idle.removeFirst().raw());
            }
            if (waiters.isEmpty() && total() < config.maxSize()) {
                creating++;
            } else {
                Lease granted = awaitResource(timeout, budget, started, toClose);
                if (granted != null) {
                    return granted;
                }
            }
        } finally {
            lock.unlock();
            closeAll(toClose);
        }
        return createConnection();
    }

    @Override
    public void resize(int maxSize) {
        List<RawConnection> toClose = new ArrayList<>();
        lock.lock();
        try {
            PoolConfig updated = config.withMaxSize(maxSize);
            checkRunning();
            config = updated;
            while (total() > maxSize && !idle.isEmpty()) {
                scheduleClose(idle.removeFirst().raw(), toClose);
            }
            dispatch();
            idleChanged.signal();
        } finally {
            lock.unlock();
            closeAll(toClose);
        }
    }

    @Override
    public PoolConfig config() {
        lock.lock();
        try {
            return config;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(total(), idle.size(), borrowed.size(), creating, waiters.size());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        long budget = checkedNanos(grace);
        long started = System.nanoTime();
        List<RawConnection> toClose = new ArrayList<>();
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            for (Waiter waiter : waiters) {
                waiter.ready.signal();
            }
            idleChanged.signal();
            while (!idle.isEmpty()) {
                scheduleClose(idle.removeFirst().raw(), toClose);
            }
        } finally {
            lock.unlock();
        }
        closeAll(toClose);
        toClose.clear();
        boolean interrupted = Thread.interrupted();
        lock.lock();
        try {
            while (!borrowed.isEmpty() && !interrupted) {
                long remaining = budget - (System.nanoTime() - started);
                if (remaining <= 0) {
                    break;
                }
                try {
                    stateChanged.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            for (Lease connection : borrowed) {
                connection.active = false;
                scheduleClose(connection.raw, toClose);
            }
            borrowed.clear();
        } finally {
            lock.unlock();
        }
        closeAll(toClose);
        lock.lock();
        try {
            while (closing != 0) {
                try {
                    stateChanged.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            lock.unlock();
        }
        boolean joined = false;
        while (!joined) {
            try {
                cleaner.join();
                joined = true;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (Thread.interrupted() || interrupted) {
            throw new InterruptedException();
        }
    }

    private Lease awaitResource(Duration timeout, long budget, long started, List<RawConnection> toClose)
            throws InterruptedException, PoolTimeoutException {
        if (budget == 0) {
            throw new PoolTimeoutException(timeout);
        }
        Waiter waiter = new Waiter(started, budget);
        waiters.addLast(waiter);
        dispatch();
        try {
            while (waiter.lease == null && !waiter.creating) {
                checkRunning();
                long remaining = budget - (System.nanoTime() - started);
                if (remaining <= 0) {
                    throw new PoolTimeoutException(timeout);
                }
                waiter.ready.awaitNanos(remaining);
            }
            checkRunning();
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } catch (InterruptedException | PoolTimeoutException | IllegalStateException e) {
            waiters.remove(waiter);
            if (waiter.creating) {
                creating--;
            }
            if (waiter.lease != null) {
                release(waiter.lease, toClose);
            }
            dispatch();
            throw e;
        }
        return waiter.lease;
    }

    private PooledConnection createConnection() throws ConnectionCreationException {
        RawConnection raw;
        try {
            raw = Objects.requireNonNull(factory.create(), "Фабрика вернула null");
        } catch (Exception e) {
            lock.lock();
            try {
                creating--;
                dispatch();
            } finally {
                lock.unlock();
            }
            throw new ConnectionCreationException(e);
        }
        List<RawConnection> toClose = new ArrayList<>();
        lock.lock();
        try {
            creating--;
            if (stopped) {
                scheduleClose(raw, toClose);
                throw new IllegalStateException("Пул остановлен");
            }
            return lease(raw);
        } finally {
            lock.unlock();
            closeAll(toClose);
        }
    }

    private void cleanIdle() {
        while (true) {
            List<RawConnection> toClose = new ArrayList<>();
            lock.lock();
            try {
                while (!stopped && toClose.isEmpty()) {
                    if (idle.size() <= config.minIdle()) {
                        idleChanged.await();
                    } else {
                        long remaining = idleTimeoutNanos - (System.nanoTime() - idle.getFirst().since());
                        if (remaining > 0) {
                            idleChanged.awaitNanos(remaining);
                        } else {
                            do {
                                scheduleClose(idle.removeFirst().raw(), toClose);
                            } while (idle.size() > config.minIdle()
                                    && System.nanoTime() - idle.getFirst().since() >= idleTimeoutNanos);
                            dispatch();
                        }
                    }
                }
                if (stopped) {
                    return;
                }
            } catch (InterruptedException e) {
                continue;
            } finally {
                lock.unlock();
            }
            closeAll(toClose);
        }
    }

    private void dispatch() {
        while (!stopped && !waiters.isEmpty()) {
            Waiter waiter = waiters.getFirst();
            if (System.nanoTime() - waiter.started >= waiter.budget) {
                waiters.removeFirst();
                waiter.ready.signal();
                continue;
            }
            if (!idle.isEmpty()) {
                waiter.lease = lease(idle.removeFirst().raw());
            } else if (total() < config.maxSize()) {
                creating++;
                waiter.creating = true;
            } else {
                break;
            }
            waiters.removeFirst();
            waiter.ready.signal();
        }
    }

    private Lease lease(RawConnection raw) {
        Lease connection = new Lease(raw);
        borrowed.add(connection);
        idleChanged.signal();
        return connection;
    }

    private void release(Lease connection, List<RawConnection> toClose) {
        if (!connection.active) {
            return;
        }
        connection.active = false;
        boolean discard = stopped || total() > config.maxSize();
        borrowed.remove(connection);
        if (discard) {
            scheduleClose(connection.raw, toClose);
        } else {
            idle.addLast(new IdleConnection(connection.raw, System.nanoTime()));
        }
        dispatch();
        idleChanged.signal();
        stateChanged.signalAll();
    }

    private void scheduleClose(RawConnection raw, List<RawConnection> toClose) {
        closing++;
        toClose.add(raw);
    }

    private void closeAll(List<RawConnection> connections) {
        for (RawConnection raw : connections) {
            try {
                raw.close();
            } catch (Exception ignored) {
            } finally {
                lock.lock();
                try {
                    closing--;
                    stateChanged.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private int total() {
        return idle.size() + borrowed.size() + creating;
    }

    private void checkRunning() {
        if (stopped) {
            throw new IllegalStateException("Пул остановлен");
        }
    }

    private static long checkedNanos(Duration duration) {
        Objects.requireNonNull(duration, "Длительность не задана");
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Длительность не может быть отрицательной");
        }
        return nanos(duration);
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private record IdleConnection(RawConnection raw, long since) {
    }

    private final class Waiter {
        private final Condition ready = lock.newCondition();
        private final long started;
        private final long budget;
        private Lease lease;
        private boolean creating;

        private Waiter(long started, long budget) {
            this.started = started;
            this.budget = budget;
        }
    }

    private final class Lease implements PooledConnection {
        private final RawConnection raw;
        private boolean active = true;

        private Lease(RawConnection raw) {
            this.raw = raw;
        }

        @Override
        public RawConnection raw() {
            lock.lock();
            try {
                if (!active) {
                    throw new IllegalStateException("Соединение возвращено или закрыто");
                }
                return raw;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            List<RawConnection> toClose = new ArrayList<>();
            lock.lock();
            try {
                release(this, toClose);
            } finally {
                lock.unlock();
                closeAll(toClose);
            }
        }
    }
}
