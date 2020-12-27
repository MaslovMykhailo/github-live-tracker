import io.r2dbc.mssql.MssqlConnectionConfiguration;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class App {

    public static void main(String[] args) throws InterruptedException {
        String host = "localhost";
        int port = 7000;

        MssqlConnectionConfiguration configuration = MssqlConnectionConfiguration.builder()
            .build();

        Thread service = new Thread(() -> new Service(host, port, configuration).run());
        service.start();

        List<Thread> units = IntStream
            .range(0, 3)
            .mapToObj(i -> new Thread(() -> new Unit(host, port, configuration).run()))
            .collect(Collectors.toList());

        units.forEach(Thread::start);

        service.join();
        for (Thread unit : units) {
            unit.join();
        }
    }

}
