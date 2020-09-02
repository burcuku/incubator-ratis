package org.apache.ratis.inst.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class FileLogger {

    public static final Object lock = new Object();

    public static void logToFile(String folderName, String fileName, String message, boolean append) {
        synchronized (lock) { // currently all logs are written to the same file
            //Date date = new Date();
            //String traceFile = new SimpleDateFormat("yyyyMMddHHmm'-trace.txt'").format(date);
            try {
                File folder = new File(folderName);
                if (!folder.exists())
                    folder.mkdir();
                FileWriter fileWriter = new FileWriter(folderName + File.separator + fileName, append);
                PrintWriter printWriter = new PrintWriter(fileWriter);
                printWriter.println(message);
                printWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
