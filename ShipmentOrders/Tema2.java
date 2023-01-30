import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Tema2 {
    public static void main(String[] args) throws IOException, InterruptedException {
        Integer numberOfThreads = Integer.parseInt(args[1]);
        String filePathOrders = args[0] + "/orders.txt";
        String filePathProducts = args[0] + "/order_products.txt";

        ExecutorService tpe = Executors.newFixedThreadPool(numberOfThreads);
        ExecutorService tpeS = Executors.newFixedThreadPool(numberOfThreads);
        CyclicBarrier barrier = new CyclicBarrier(numberOfThreads);

        ArrayList<Integer> positions = positions(filePathOrders, numberOfThreads);
        ArrayList<Integer> sizeOfChunks = new ArrayList<>();

        File f = new File(args[0] + "/orders.txt");

        // filewriter used for outputing to orders.txt
        FileWriter fw = new FileWriter("orders_out.txt");
        // filewriter used for outputing to order_products_out.txt
        FileWriter fwProducts = new FileWriter("order_products_out.txt");

        long lengthOfFile = f.length();

        numberOfThreads = positions.get(positions.size() - 1);
        positions.remove(positions.size() - 1);

        for (int i = 0; i < positions.size(); i++) {
            if (i + 1 == positions.size()) {
                sizeOfChunks.add((int) (lengthOfFile - positions.get(i)));
            } else {
                sizeOfChunks.add(positions.get(i + 1) - positions.get(i));
            }

        }

        for (int i = 0; i < numberOfThreads; i++) {
            tpe.submit(
                    new FirstLevel(positions.get(i), sizeOfChunks.get(i), filePathOrders, fw, fwProducts, tpeS,
                            filePathProducts,
                            barrier));
        }

        tpe.shutdown();
        tpe.awaitTermination(10, TimeUnit.SECONDS);
        tpeS.shutdown();
        tpeS.awaitTermination(10, TimeUnit.SECONDS);
        fw.flush();
        fw.close();
        fwProducts.flush();
        fwProducts.close();
    }

    public static ArrayList<Integer> positions(String fileName, int numberOfThreads) throws IOException {
        ArrayList<Integer> positions = new ArrayList<>();

        File f = new File(fileName);

        long lengthOfFile = f.length();

        int approximateChunk = (int) ((lengthOfFile / numberOfThreads));

        while (approximateChunk < 17) {
            numberOfThreads--;
            approximateChunk = (int) ((lengthOfFile / numberOfThreads));
        }

        byte[] bufferByte = new byte[17];
        FileInputStream fr = new FileInputStream(fileName);

        positions.add(0);
        for (int i = 0; i < numberOfThreads - 1; i++) {
            fr.getChannel().position(approximateChunk * (i + 1));
            fr.read(bufferByte);
            String result = new String(bufferByte);

            int currentChunk = approximateChunk * (i + 1) + result.length();
            int offset = 0;

            while (result.charAt(result.length() - offset - 1) != '\n') {
                currentChunk--;
                offset++;
            }
            positions.add(currentChunk);
        }

        positions.add(numberOfThreads);

        fr.close();
        return positions;
    }
}

class FirstLevel implements Runnable {
    Integer position;
    Integer chunkSize;
    Integer upperLimit;
    String filePath;
    FileWriter fw;
    FileWriter fwProducts;
    ExecutorService tpe;
    String filePathProducts;
    CyclicBarrier barrier;

    public FirstLevel(Integer position, Integer chunkSize, String filePath, FileWriter fw, FileWriter fwProducts,
            ExecutorService tpe,
            String filePathProducts, CyclicBarrier barrier) {
        this.position = position;
        this.chunkSize = chunkSize;
        this.filePath = filePath;
        this.upperLimit = position + chunkSize;
        this.fw = fw;
        this.fwProducts = fwProducts;
        this.tpe = tpe;
        this.filePathProducts = filePathProducts;
        this.barrier = barrier;
    }

    public List<Integer> positionsProducts(String filePath, int numberOfItems) throws IOException {
        List<Integer> positions = new ArrayList<>();

        File f = new File(filePath);

        long lengthOfFile = f.length();

        int approximateChunk = (int) ((lengthOfFile / numberOfItems));

        byte[] bufferByte = new byte[26];
        FileInputStream fr = new FileInputStream(filePath);

        positions.add(0);

        for (int i = 0; i < numberOfItems - 1; i++) {
            fr.getChannel().position(approximateChunk * (i + 1));
            fr.read(bufferByte);
            String result = new String(bufferByte);

            int currentChunk = approximateChunk * (i + 1) + result.length();
            int offset = 0;

            while (result.charAt(result.length() - offset - 1) != '\n') {
                currentChunk--;
                offset++;
            }

            positions.add(currentChunk);
        }

        fr.close();

        return positions;
    }

    List<Integer> chunksProducts(List<Integer> positions, String filePath) {
        List<Integer> chunks = new ArrayList<>();

        File f = new File(filePath);
        long lengthOfFile = f.length();

        for (int i = 0; i < positions.size(); i++) {
            if (i + 1 == positions.size()) {
                chunks.add((int) (lengthOfFile - positions.get(i)));
            } else {
                chunks.add(positions.get(i + 1) - positions.get(i));
            }

        }

        return chunks;
    }

    @Override
    public void run() {
        Integer currentPosition = position;
        Integer lineChunk = 17;
        try (FileInputStream fr = new FileInputStream(this.filePath)) {
            while (currentPosition < upperLimit) {
                fr.getChannel().position(currentPosition);
                byte[] bufferByte = new byte[lineChunk];
                fr.read(bufferByte);
                String result = new String(bufferByte);
                int offset = 0;
                while (result.charAt(result.length() - offset - 1) != '\n') {
                    offset++;
                }
                String oneLine = result.substring(0, result.length() - offset - 1).trim();
                String[] parsedLine = oneLine.split(",");
                Integer numberOfItems = Integer.parseInt(parsedLine[1]);
                if (numberOfItems != 0) {

                    List<Integer> positionList = positionsProducts(filePathProducts, numberOfItems);
                    List<Integer> chunkList = chunksProducts(positionList, filePathProducts);
                    List<Future<Integer>> results = new ArrayList<Future<Integer>>();
                    for (int i = 0; i < numberOfItems; i++) {
                        results.add(tpe.submit(new SecondLevel(parsedLine[0], positionList.get(i), chunkList.get(i),
                                filePathProducts, fwProducts)));

                    }

                    for (Future<Integer> future : results) {
                        future.get();
                    }

                    synchronized (this.fw) {
                        fw.write(oneLine + ",shipped\n");
                    }
                }

                currentPosition = currentPosition + (lineChunk - offset);
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}

class SecondLevel implements Callable<Integer> {
    String id;
    Integer position;
    Integer chunk;
    String filePath;
    FileWriter fw;

    public SecondLevel(String id, Integer position, Integer chunk, String filePath, FileWriter fw) {
        this.id = id;
        this.position = position;
        this.chunk = chunk;
        this.filePath = filePath;
        this.fw = fw;
    }

    @Override
    public Integer call() throws Exception {
        Integer currentPosition = position;
        Integer lineChunk = 26;
        Integer upperLimit = position + chunk;
        FileInputStream fr = new FileInputStream(this.filePath);
        while (currentPosition < upperLimit) {

            fr.getChannel().position(currentPosition);
            byte[] bufferByte = new byte[lineChunk];
            fr.read(bufferByte);
            String result = new String(bufferByte);
            String[] parsedLine = result.trim().split(",");

            if (parsedLine[0].equals(id)) {
                synchronized (this.fw) {
                    fw.write(parsedLine[0] + "," + parsedLine[1] + ",shipped\n");
                }
            }

            currentPosition = currentPosition + (lineChunk);
        }
        fr.close();
        return null;
    }

}