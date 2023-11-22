import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class ExManager {
    private String path;
    private int num_of_nodes;
    // your code here
    private HashMap<Integer, Node> nodes;

    public ExManager(String path){
        this.path = path;
        // your code here
        nodes = new HashMap<>();
    }

    public Node get_node(int id){
        // your code here
        return this.nodes.get(id-1);
    }

    public int getNum_of_nodes() {
        return this.num_of_nodes;
    }

    public void update_edge(int id1, int id2, double weight){
        //your code here
        Node node = this.nodes.get(id1-1);
        node.update_weight(id1-1,id2-1,weight);
        node = this.nodes.get(id2-1);
        node.update_weight(id1-1,id2-1,weight);
    }

    public void read_txt(){
        // your code here
        try {
            Scanner scanner = new Scanner(new File(path));
            String line = scanner.nextLine();
            String[] data = line.split(" ");
            this.num_of_nodes = Integer.parseInt(data[0]);
            line = scanner.nextLine();
            data = line.split(" ");

            while(!line.contains("stop")) {
                int node_key = Integer.parseInt(data[0]) - 1;
                Node node = this.nodes.get(node_key);
                if(node == null){
                    node = new Node(node_key, this.num_of_nodes);
                    this.nodes.put(node_key,node);
                }
                int index = 1;
                while(index < data.length){
                    int next_node_key = Integer.parseInt(data[index]) - 1;
                    Node next_node = this.nodes.get(next_node_key);
                    if(next_node == null){
                        next_node = new Node(next_node_key, this.num_of_nodes);
                        this.nodes.put(next_node_key,next_node);
                    }
                    index ++;
                    node.update_weight(node_key, next_node_key, Double.parseDouble(data[index]));
                    index ++;
                    node.add_neighbor(next_node_key, Integer.parseInt(data[index]), Integer.parseInt(data[index+1]));
                    index += 2;
                }
                line = scanner.nextLine();
                data = line.split(" ");
            }
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void start(){
        // your code here
        for (int key_node : this.nodes.keySet()) {
            this.nodes.get(key_node).open_servers();
        }
        for (int key_node : this.nodes.keySet()) {
            this.nodes.get(key_node).open_sockets();
        }
        for (int key_node : this.nodes.keySet()) {
            this.nodes.get(key_node).connect_servers();
        }
        ArrayList<Pair<Thread, Integer>> threads = new ArrayList<>();
        for(int key_node : this.nodes.keySet()){
            Thread thread = new Thread(this.nodes.get(key_node));
            thread.start();
            threads.add(new Pair<>(thread, key_node));
        }
        try {
            for (Pair<Thread, Integer> thread : threads) {
                thread.getKey().join();
            }
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }
        for (Pair<Thread, Integer> pair : threads) {
            this.nodes.get(pair.getValue()).reset_messages();
        }
        this.terminate();
    }

    public void terminate(){
        //your code here
        for (int key_node : this.nodes.keySet()){
            this.nodes.get(key_node).kill();
        }
    }
}
