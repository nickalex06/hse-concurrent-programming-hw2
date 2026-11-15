package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class ConnectionPoolConcurrencyTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test
    void interruptionCancelsConnectionAssignedBeforeWakeup() throws Exception {
        checkInterruptedAssignment(false);
    }

    @Test
    void interruptionCancelsSlotAssignedBeforeWakeup() throws Exception {
        checkInterruptedAssignment(true);
    }

    private void checkInterruptedAssignment(boolean grow) throws Exception {
        TrackingFactory factory = new TrackingFactory();
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, factory);
        PooledConnection held = pool.borrow(WAIT);
        ReentrantLock lock = poolLock(pool);
        FutureTask<Void> result = new FutureTask<>(() -> {
            assertThrows(InterruptedException.class, () -> pool.borrow(WAIT));
            return null;
        });
        Thread waiter = new Thread(result);
        waiter.start();
        try {
            await(() -> pool.stats().waiting() == 1);
            lock.lock();
            try {
                if (grow) {
                    pool.resize(2);
                } else {
                    held.close();
                }
                waiter.interrupt();
            } finally {
                lock.unlock();
            }
            result.get(2, TimeUnit.SECONDS);
            assertEquals(0, pool.stats().creating());
            assertEquals(0, pool.stats().waiting());
            held.close();
            assertEquals(new PoolStats(1, 1, 0, 0, 0), pool.stats());
            assertEquals(1, factory.connections.size());
            pool.borrow(Duration.ZERO).close();
        } finally {
            waiter.interrupt();
            waiter.join(2000);
            held.close();
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void interruptionWhileAcquiringPoolLockDoesNotReserveCapacity() throws Exception {
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, new TrackingFactory());
        ReentrantLock lock = poolLock(pool);
        FutureTask<Void> result = new FutureTask<>(() -> {
            assertThrows(InterruptedException.class, () -> pool.borrow(WAIT));
            return null;
        });
        Thread waiter = new Thread(result);
        lock.lock();
        try {
            waiter.start();
            await(() -> lock.hasQueuedThread(waiter));
            waiter.interrupt();
            result.get(2, TimeUnit.SECONDS);
            assertEquals(new PoolStats(0, 0, 0, 0, 0), pool.stats());
        } finally {
            lock.unlock();
            waiter.interrupt();
            waiter.join(2000);
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void expiredWaiterCannotReceiveReturnedConnection() throws Exception {
        checkExpiredWaiter(false);
    }

    @Test
    void expiredWaiterCannotReserveNewSlot() throws Exception {
        checkExpiredWaiter(true);
    }

    private void checkExpiredWaiter(boolean grow) throws Exception {
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, new TrackingFactory());
        PooledConnection held = pool.borrow(WAIT);
        ReentrantLock lock = poolLock(pool);
        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<?> waiter = executor.submit(() -> assertThrows(PoolTimeoutException.class,
                    () -> pool.borrow(Duration.ofMillis(200))));
            await(() -> pool.stats().waiting() == 1);
            lock.lock();
            try {
                Thread.sleep(250);
                if (grow) {
                    pool.resize(2);
                } else {
                    held.close();
                }
            } finally {
                lock.unlock();
            }
            waiter.get(2, TimeUnit.SECONDS);
            assertEquals(0, pool.stats().creating());
            assertEquals(0, pool.stats().waiting());
            held.close();
            pool.borrow(Duration.ZERO).close();
        } finally {
            held.close();
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void concurrentBorrowNeverSharesRawConnections() throws Exception {
        TrackingFactory factory = new TrackingFactory();
        ConnectionPool pool = pool(0, 5, Duration.ofMinutes(1), true, factory);
        Set<RawConnection> inUse = ConcurrentHashMap.newKeySet();
        try (var executor = Executors.newFixedThreadPool(24)) {
            List<Future<?>> tasks = new ArrayList<>();
            CyclicBarrier start = new CyclicBarrier(24);
            for (int i = 0; i < 24; i++) {
                tasks.add(executor.submit(() -> {
                    start.await();
                    for (int j = 0; j < 100; j++) {
                        PooledConnection connection = pool.borrow(WAIT);
                        RawConnection raw = connection.raw();
                        assertTrue(inUse.add(raw));
                        PoolStats stats = pool.stats();
                        assertEquals(stats.total(), stats.idle() + stats.borrowed() + stats.creating());
                        assertTrue(stats.total() <= 5);
                        assertTrue(inUse.remove(raw));
                        connection.close();
                        connection.close();
                        assertThrows(IllegalStateException.class, connection::raw);
                    }
                    return null;
                }));
            }
            for (Future<?> task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            assertEquals(0, pool.stats().borrowed());
            assertEquals(0, pool.stats().waiting());
        } finally {
            pool.shutdown(Duration.ZERO);
        }
        factory.connections.forEach(connection -> assertEquals(1, connection.closes.get()));
    }

    @Test
    void fairWaitersReceiveReturnedConnectionInOrder() throws Exception {
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, new TrackingFactory());
        PooledConnection held = pool.borrow(WAIT);
        ConcurrentLinkedQueue<Integer> order = new ConcurrentLinkedQueue<>();
        try (var executor = Executors.newFixedThreadPool(10)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                int index = i;
                tasks.add(executor.submit(() -> {
                    try (PooledConnection ignored = pool.borrow(WAIT)) {
                        order.add(index);
                    }
                    return null;
                }));
                int waiting = i + 1;
                await(() -> pool.stats().waiting() == waiting);
            }
            held.close();
            for (Future<?> task : tasks) {
                task.get(5, TimeUnit.SECONDS);
            }
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), new ArrayList<>(order));
        } finally {
            held.close();
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void slowCreationDoesNotHoldPoolLockOrUseBorrowTimeout() throws Exception {
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        ConnectionPool pool = pool(0, 2, Duration.ofMinutes(1), true, () -> {
            if (calls.incrementAndGet() == 2) {
                creating.countDown();
                finish.await();
            }
            return new TrackedConnection();
        });
        PooledConnection first = pool.borrow(WAIT);
        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<PooledConnection> second = executor.submit(() -> pool.borrow(Duration.ZERO));
            try {
                assertTrue(creating.await(2, TimeUnit.SECONDS));
                assertEquals(new PoolStats(2, 0, 1, 1, 0), pool.stats());
                first.close();
                try (PooledConnection reused = pool.borrow(Duration.ZERO)) {
                    assertNotNull(reused.raw());
                }
            } finally {
                finish.countDown();
            }
            second.get(2, TimeUnit.SECONDS).close();
        } finally {
            finish.countDown();
            first.close();
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void failedCreationHandsItsReservationToWaiter() throws Exception {
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch fail = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        Exception failure = new Exception("Ошибка создания соединения");
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, () -> {
            if (calls.incrementAndGet() == 1) {
                creating.countDown();
                fail.await();
                throw failure;
            }
            return new TrackedConnection();
        });
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<?> creator = executor.submit(() -> {
                assertSame(failure, assertThrows(ConnectionCreationException.class,
                        () -> pool.borrow(WAIT)).getCause());
            });
            try {
                assertTrue(creating.await(2, TimeUnit.SECONDS));
                Future<PooledConnection> waiter = executor.submit(() -> pool.borrow(WAIT));
                await(() -> pool.stats().waiting() == 1);
                fail.countDown();
                creator.get(2, TimeUnit.SECONDS);
                waiter.get(2, TimeUnit.SECONDS).close();
                assertEquals(new PoolStats(1, 1, 0, 0, 0), pool.stats());
            } finally {
                fail.countDown();
            }
        } finally {
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void interruptAndTimeoutRemoveWaitersWithoutLosingCapacity() throws Exception {
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, new TrackingFactory());
        PooledConnection held = pool.borrow(WAIT);
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<?> interrupted = executor.submit(() -> {
                assertThrows(InterruptedException.class, () -> pool.borrow(WAIT));
            });
            await(() -> pool.stats().waiting() == 1);
            Future<?> timedOut = executor.submit(() -> {
                assertThrows(PoolTimeoutException.class, () -> pool.borrow(Duration.ofMillis(100)));
            });
            timedOut.get(2, TimeUnit.SECONDS);
            assertEquals(1, pool.stats().waiting());
            interrupted.cancel(true);
            await(() -> pool.stats().waiting() == 0);
            held.close();
            pool.borrow(Duration.ZERO).close();
            assertEquals(new PoolStats(1, 1, 0, 0, 0), pool.stats());
        } finally {
            held.close();
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void resizeWakesWaitersAndClosesExcessOnReturn() throws Exception {
        TrackingFactory factory = new TrackingFactory();
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, factory);
        PooledConnection first = pool.borrow(WAIT);
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<PooledConnection> second = executor.submit(() -> pool.borrow(WAIT));
            Future<PooledConnection> third = executor.submit(() -> pool.borrow(WAIT));
            await(() -> pool.stats().waiting() == 2);
            pool.resize(3);
            PooledConnection b = second.get(2, TimeUnit.SECONDS);
            PooledConnection c = third.get(2, TimeUnit.SECONDS);
            pool.resize(1);
            assertEquals(3, pool.stats().total());
            first.close();
            b.close();
            c.close();
            assertEquals(new PoolStats(1, 1, 0, 0, 0), pool.stats());
            assertEquals(2, factory.connections.stream().filter(raw -> raw.closes.get() == 1).count());
        } finally {
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void slowCloseDoesNotBlockBorrowOrStatsAndShutdownWaitsForIt() throws Exception {
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        ConnectionPool pool = pool(0, 2, Duration.ofMinutes(1), false, () -> {
            if (calls.incrementAndGet() == 1) {
                return () -> {
                    closing.countDown();
                    try {
                        finish.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new IllegalStateException("Ошибка закрытия соединения");
                };
            }
            return new TrackedConnection();
        });
        PooledConnection first = pool.borrow(WAIT);
        PooledConnection second = pool.borrow(WAIT);
        first.close();
        second.close();
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<?> resize = executor.submit(() -> pool.resize(1));
            try {
                assertTrue(closing.await(2, TimeUnit.SECONDS));
                assertEquals(new PoolStats(1, 1, 0, 0, 0), pool.stats());
                pool.borrow(Duration.ZERO).close();
                Future<?> shutdown = executor.submit(() -> {
                    pool.shutdown(Duration.ZERO);
                    return null;
                });
                await(() -> pool.stats().total() == 0);
                assertFalse(shutdown.isDone());
                finish.countDown();
                resize.get(2, TimeUnit.SECONDS);
                shutdown.get(2, TimeUnit.SECONDS);
            } finally {
                finish.countDown();
            }
        } finally {
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void idleEvictionPreservesMinIdleWithoutCreatingConnections() throws Exception {
        TrackingFactory factory = new TrackingFactory();
        ConnectionPool pool = pool(1, 3, Duration.ofMillis(50), false, factory);
        try {
            assertTrue(factory.connections.isEmpty());
            List<PooledConnection> connections = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                connections.add(pool.borrow(WAIT));
            }
            connections.forEach(PooledConnection::close);
            long returned = System.nanoTime();
            await(() -> factory.connections.stream().mapToInt(raw -> raw.closes.get()).sum() == 2);
            assertTrue(System.nanoTime() - returned < TimeUnit.MILLISECONDS.toNanos(550));
            assertEquals(new PoolStats(1, 1, 0, 0, 0), pool.stats());
        } finally {
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void shutdownRejectsWaitersAndLetsBorrowerReturnDuringGrace() throws Exception {
        TrackingFactory factory = new TrackingFactory();
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), true, factory);
        PooledConnection held = pool.borrow(WAIT);
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<?> waiter = executor.submit(() -> {
                assertThrows(IllegalStateException.class, () -> pool.borrow(WAIT));
            });
            await(() -> pool.stats().waiting() == 1);
            Future<?> shutdown = executor.submit(() -> {
                pool.shutdown(WAIT);
                return null;
            });
            waiter.get(2, TimeUnit.SECONDS);
            assertFalse(shutdown.isDone());
            assertNotNull(held.raw());
            held.close();
            shutdown.get(2, TimeUnit.SECONDS);
            assertEquals(new PoolStats(0, 0, 0, 0, 0), pool.stats());
            assertEquals(1, factory.connections.element().closes.get());
        } finally {
            held.close();
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void shutdownDoesNotWaitForFactoryAndClosesItsLateResult() throws Exception {
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        TrackedConnection raw = new TrackedConnection();
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), false, () -> {
            creating.countDown();
            finish.await();
            return raw;
        });
        try (var executor = Executors.newSingleThreadExecutor()) {
            Future<?> creator = executor.submit(() -> {
                assertThrows(IllegalStateException.class, () -> pool.borrow(WAIT));
            });
            try {
                assertTrue(creating.await(2, TimeUnit.SECONDS));
                pool.shutdown(Duration.ZERO);
                assertEquals(new PoolStats(1, 0, 0, 1, 0), pool.stats());
            } finally {
                finish.countDown();
            }
            creator.get(2, TimeUnit.SECONDS);
            assertEquals(1, raw.closes.get());
            assertEquals(new PoolStats(0, 0, 0, 0, 0), pool.stats());
        } finally {
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void interruptedShutdownForceClosesAndStillTerminates() throws Exception {
        TrackingFactory factory = new TrackingFactory();
        ConnectionPool pool = pool(0, 1, Duration.ofMinutes(1), false, factory);
        PooledConnection held = pool.borrow(WAIT);
        CountDownLatch done = new CountDownLatch(1);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        Thread shutdown = new Thread(() -> {
            try {
                assertThrows(InterruptedException.class, () -> pool.shutdown(Duration.ofMinutes(1)));
            } catch (Throwable e) {
                failures.add(e);
            } finally {
                done.countDown();
            }
        });
        shutdown.start();
        try {
            await(() -> shutdown.getState() == Thread.State.TIMED_WAITING);
            shutdown.interrupt();
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertTrue(failures.isEmpty(), failures::toString);
            assertThrows(IllegalStateException.class, held::raw);
            held.close();
            assertEquals(1, factory.connections.element().closes.get());
        } finally {
            shutdown.interrupt();
            shutdown.join(2000);
            pool.shutdown(Duration.ZERO);
        }
    }

    @Test
    void validatesArgumentsBeforeStoppedStateAndAcceptsHugeDurations() throws Exception {
        ConnectionPool pool = pool(1, 2, Duration.ofSeconds(Long.MAX_VALUE), false, new TrackingFactory());
        pool.borrow(Duration.ofSeconds(Long.MAX_VALUE)).close();
        pool.shutdown(Duration.ZERO);
        assertThrows(NullPointerException.class, () -> pool.borrow(null));
        assertThrows(IllegalArgumentException.class, () -> pool.borrow(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> pool.resize(0));
        assertThrows(NullPointerException.class, () -> pool.shutdown(null));
        assertThrows(IllegalArgumentException.class, () -> pool.shutdown(Duration.ofNanos(-1)));
        assertThrows(IllegalStateException.class, () -> pool.borrow(Duration.ZERO));
    }

    static ConnectionPool pool(int minIdle, int maxSize, Duration idleTimeout,
                               boolean fair, ConnectionFactory factory) {
        return new ConnectionPoolImpl(new PoolConfig(minIdle, maxSize, idleTimeout, fair), factory);
    }

    private static ReentrantLock poolLock(ConnectionPool pool) throws ReflectiveOperationException {
        var field = ConnectionPoolImpl.class.getDeclaredField("lock");
        field.setAccessible(true);
        return (ReentrantLock) field.get(pool);
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                fail("Ожидаемое состояние не достигнуто за отведённое время");
            }
            Thread.sleep(1);
        }
    }

    private static final class TrackingFactory implements ConnectionFactory {
        private final ConcurrentLinkedQueue<TrackedConnection> connections = new ConcurrentLinkedQueue<>();

        @Override
        public RawConnection create() {
            TrackedConnection connection = new TrackedConnection();
            connections.add(connection);
            return connection;
        }
    }

    private static final class TrackedConnection implements RawConnection {
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }
}
