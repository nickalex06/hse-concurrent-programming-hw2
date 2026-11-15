package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.hse.concurrency.pool.ConnectionPoolConcurrencyTest.await;
import static edu.hse.concurrency.pool.ConnectionPoolConcurrencyTest.pool;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class PoolStarvationTest {

    private static final Duration BORROW_TIMEOUT = Duration.ofMinutes(1);

    @Test
    void nestedBorrowStarvesAndProducesThreadDump() throws Exception {
        int threads = 4;
        ConnectionPool pool = pool(0, threads, Duration.ofMinutes(5), true, () -> () -> {});
        CyclicBarrier firstConnectionsAcquired = new CyclicBarrier(threads);
        AtomicInteger threadIndex = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(threads,
                task -> new Thread(task, "pool-starvation-" + threadIndex.incrementAndGet()));
        List<Future<?>> requests = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                requests.add(executor.submit(() -> {
                    try (PooledConnection first = pool.borrow(BORROW_TIMEOUT)) {
                        firstConnectionsAcquired.await(5, TimeUnit.SECONDS);
                        try (PooledConnection second = pool.borrow(BORROW_TIMEOUT)) {
                            fail("Все соединения должны удерживаться запросами, ожидающими второе соединение");
                        }
                    }
                    return null;
                }));
            }
            await(() -> pool.stats().waiting() == threads);
            assertEquals(new PoolStats(threads, 0, threads, 0, threads), pool.stats());
            assertTrue(requests.stream().noneMatch(Future::isDone));
            assertNull(ManagementFactory.getThreadMXBean().findDeadlockedThreads());
            Path dump = captureThreadDump();
            String text = Files.readString(dump).replace("\r\n", "\n");
            for (int i = 1; i <= threads; i++) {
                int start = text.indexOf('"' + "pool-starvation-" + i + '"');
                assertTrue(start >= 0);
                int end = text.indexOf("\n\n", start);
                String stack = text.substring(start, end < 0 ? text.length() : end);
                assertTrue(stack.contains("TIMED_WAITING"), stack);
                assertTrue(stack.contains("ConnectionPoolImpl.awaitResource"), stack);
            }
            assertFalse(text.contains("Found one Java-level deadlock"));
            assertEquals(threads, pool.stats().waiting());
        } finally {
            requests.forEach(request -> request.cancel(true));
            executor.shutdownNow();
            try {
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                pool.shutdown(Duration.ZERO);
            }
        }
        assertEquals(new PoolStats(0, 0, 0, 0, 0), pool.stats());
    }

    @Test
    void minimumCapacityLetsEveryRequestFinish() throws Exception {
        for (boolean fair : List.of(false, true)) {
            for (int threads : List.of(1, 4, 8)) {
                for (int connectionsPerRequest : List.of(1, 2, 3, 4)) {
                    checkCompletion(threads, connectionsPerRequest, fair);
                }
            }
        }
    }

    private void checkCompletion(int threads, int connectionsPerRequest, boolean fair) throws Exception {
        int maxSize = threads * (connectionsPerRequest - 1) + 1;
        ConnectionPool pool = pool(0, maxSize, Duration.ofMinutes(5), fair, () -> () -> {});
        CyclicBarrier beforeLastConnection = new CyclicBarrier(threads);
        var executor = Executors.newFixedThreadPool(threads);
        List<Future<?>> requests = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                requests.add(executor.submit(() -> {
                    List<PooledConnection> held = new ArrayList<>();
                    try {
                        for (int j = 0; j < connectionsPerRequest - 1; j++) {
                            held.add(pool.borrow(BORROW_TIMEOUT));
                        }
                        beforeLastConnection.await(5, TimeUnit.SECONDS);
                        held.add(pool.borrow(BORROW_TIMEOUT));
                        held.forEach(connection -> assertNotNull(connection.raw()));
                    } finally {
                        held.forEach(PooledConnection::close);
                    }
                    return null;
                }));
            }
            for (Future<?> request : requests) {
                request.get(5, TimeUnit.SECONDS);
            }
            assertEquals(0, pool.stats().borrowed());
            assertEquals(0, pool.stats().waiting());
            assertTrue(pool.stats().total() <= maxSize);
        } finally {
            requests.forEach(request -> request.cancel(true));
            executor.shutdownNow();
            try {
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                pool.shutdown(Duration.ZERO);
            }
        }
    }

    private Path captureThreadDump() throws Exception {
        Path directory = Path.of("build", "diagnostics");
        Files.createDirectories(directory);
        Path processes = directory.resolve("pool-starvation-jps.txt");
        runJdkTool(processes, "jps", "-l");
        String currentPid = Long.toString(ProcessHandle.current().pid());
        String pid = Files.readAllLines(processes).stream()
                .map(line -> line.trim().split("\\s+", 2)[0])
                .filter(currentPid::equals)
                .findFirst().orElseThrow(() -> new IllegalStateException("jps не нашёл процесс тестов"));
        Path dump = directory.resolve("pool-starvation-threads.txt");
        runJdkTool(dump, "jcmd", pid, "Thread.print");
        return dump;
    }

    private void runJdkTool(Path output, String tool, String... arguments) throws Exception {
        String suffix = System.getProperty("os.name").startsWith("Windows") ? ".exe" : "";
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", tool + suffix).toString());
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Истекло время выполнения " + tool);
            assertEquals(0, process.exitValue(), () -> "Ошибка выполнения " + tool + ": " + output);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
        }
    }
}
