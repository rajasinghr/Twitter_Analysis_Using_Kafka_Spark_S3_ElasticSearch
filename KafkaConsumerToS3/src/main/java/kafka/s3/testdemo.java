package kafka.s3;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class testdemo {
    public static void main(String[] args) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddhhmmss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
    }
}
