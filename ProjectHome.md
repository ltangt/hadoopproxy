# Motivation #
This project is to assist normal users who are not familiar with hadoop to utilize an existing hadoop environment to run parallel jobs. The simple idea is to make the existing hadoop enviroment be a **thread pool**.

# How to Use #
The user does not need to know how to write _map/reduce_ function or how to submit a hadoop job. The proxy is a container of threads. **The user only needs to insert his(or her) created threads into the container and invoke the execution function**. Then, the proxy will automatically send these threads to hadoop server and transform them into map/reduce functions.

For example:
```
public class Test1 {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            
            ProxyClient proxy = new ProxyClient("hadoopxxxxserver.cs.xxx.edu");
            proxy.addTask(new TestTask("a b c d e f"));
            proxy.addTask(new TestTask("ab cd ef gf"));
            proxy.addTask(new TestTask("abcc aaascd efee gfdssssf"));
            proxy.addTask(new TestTask("11 33 22 33 11 22 33 44 55"));
            proxy.execute();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static class TestTask implements ProxyTask {
      
        private String line = null;
        
        public TestTask(String line) {
            this.line = line;
        }

        public void run(PrintStream out) throws InterruptedException {
            // TODO Auto-generated method stub
            line = line.replaceAll("\\s", "==");
            out.println(line);
        }
        
    }

}
```