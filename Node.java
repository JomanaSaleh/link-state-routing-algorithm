import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Node implements Runnable{

    private int key;
    private int num_of_nodes;
    private double[][] graph_weights;
    private HashMap<Integer, Socket> sockets;
    private HashMap<Integer, ServerSocket> servers;
    private HashMap<Integer, DataOutputStream> out_data;
    private HashMap<Integer, DataInputStream> in_data;
    private ArrayList<Message> messages;
    private HashMap<Integer, Integer> out_ports;
    private HashMap<Integer, Integer> in_ports;
    private int time;
    private ArrayList<Message>[] nodes_messages;
    private HashSet<Integer> waiting_nodes;

    public Node(int key, int num_of_nodes){
        this.num_of_nodes = num_of_nodes;
        this.key = key;
        this.graph_weights = new double[num_of_nodes][num_of_nodes];
        this.sockets = new HashMap<>();
        this.servers = new HashMap<>();
        this.out_data = new HashMap<>();
        this.in_data = new HashMap<>();
        this.messages = new ArrayList<>();
        this.out_ports = new HashMap<>();
        this.in_ports = new HashMap<>();
        this.time = 0;
        this.nodes_messages = new ArrayList[this.num_of_nodes];
        for (int i = 0; i < this.num_of_nodes; i++) {
            this.nodes_messages[i] = new ArrayList<>();
        }
        this.waiting_nodes = new HashSet<>();
        for(int i = 0;i < num_of_nodes;i ++){
            if(this.key != i){
                this.waiting_nodes.add(i);
            }
            for(int j = 0;j < num_of_nodes;j ++){
                graph_weights[i][j] = -1;
            }
        }
    }

    public void send_messages(){
        while (this.messages.size() > 0) {
            Message message = this.messages.get(0);
            this.messages.remove(0);
            String mess = message.toString();
            for (int key_neighbor : this.sockets.keySet()) {
                if(message.get_node_id() == key_neighbor){
                    continue;
                }
                DataOutputStream out_neighbor = this.out_data.get(key_neighbor);
                try {
                    if (!sockets.get(key_neighbor).isClosed()) {
                        byte[] bytes = mess.getBytes();
                        int size = bytes.length;
                        byte[] mess_size = ByteBuffer.allocate(4).putInt(size).array();
                        out_neighbor.write(mess_size);
                        out_neighbor.write(bytes);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void receive_messages(){
        for (int key_neighbor : this.servers.keySet()) {
            DataInputStream in_neighbor = this.in_data.get(key_neighbor);
            try {
                if (this.servers.get(key_neighbor).isBound()) {
                    if (in_neighbor.available() > 0) {
                        byte[] bytes_size = in_neighbor.readNBytes(4);
                        int size = ByteBuffer.wrap(bytes_size).getInt();
                        byte[] bytes = in_neighbor.readNBytes(size);
                        String mess = new String(bytes, StandardCharsets.UTF_8);
                        Message converted_message = new Message(mess);
                        this.update_message(converted_message);
                    }
                }
            } catch (IOException e){
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run() {
        this.update_max_time();
        this.time = 0;
        if(this.messages.size() == 0){
            Message message = new Message(key, key, -1, this.num_of_nodes, 0, 0, this.key);
            this.messages.add(message);
        }
        while (this.waiting_nodes.size() > 0){
            send_messages();
            receive_messages();
        }
        send_messages();
        this.nodes_messages = new ArrayList[this.num_of_nodes];
        for (int i = 0; i < this.num_of_nodes; i++) {
            this.nodes_messages[i] = new ArrayList<>();
        }
        this.waiting_nodes = new HashSet<>();
        for(int i = 0;i < num_of_nodes;i ++){
            if(this.key != i){
                this.waiting_nodes.add(i);
            }
        }
    }

    public void reset_messages(){
        this.messages = new ArrayList<>();
    }

    public void update_neighbor_list(Message message){
        if(message.get_max_time() == 0){
            this.nodes_messages[message.get_node_id()].add(message);
            this.waiting_nodes.remove(message.get_node_id());
            return;
        }
        boolean inside = false;
        for(Message updated_message : this.nodes_messages[message.get_node_id()]){
            if(updated_message.get_time_sent() == message.get_time_sent()){
                inside = true;
            }
        }
        if(!inside){
            this.nodes_messages[message.get_node_id()].add(message);
            if(this.nodes_messages[message.get_node_id()].size() == message.get_max_time()){
                this.waiting_nodes.remove(message.get_node_id());
            }
        }
    }

    public void update_message(Message message){
        if(message.get_max_time() > 0) {
            Pair<Integer, Integer> nodes = message.get_edge();
            this.graph_weights[nodes.getKey()][nodes.getValue()] = message.get_weight();
            this.graph_weights[nodes.getValue()][nodes.getKey()] = message.get_weight();
        }
        boolean already_sent = false;
        for(Message mess : this.nodes_messages[message.get_node_id()]){
            if(mess.get_edge().getKey() == message.get_edge().getKey() && mess.get_edge().getValue() == message.get_edge().getValue()){
                already_sent = true;
            }
        }
        if(!already_sent) {
            this.messages.add(message);
            this.update_neighbor_list(message);
        }
    }

    public void update_weight(int node1, int node2, double weight){
        this.graph_weights[node1][node2] = weight;
        this.graph_weights[node2][node1] = weight;
        if(node2 < node1){
            int helper = node2;
            node2 = node1;
            node1 = helper;
        }
        Message message = new Message(node1, node2, weight, this.num_of_nodes, this.time, 0, this.key);
        this.time++;
        boolean in_messages = false;
        for (int i = 0; i < this.messages.size(); i++) {
            if (this.messages.get(i).get_edge().getKey() == message.get_edge().getKey() && this.messages.get(i).get_edge().getValue() == message.get_edge().getValue()) {
                this.messages.set(i, message);
                in_messages = true;
            }
        }
        if (!in_messages) {
            this.messages.add(message);
        }
    }

    public void update_max_time(){
        for (int i = 0;i < this.messages.size();i ++){
            this.messages.get(i).set_max_time(this.messages.size());
        }
    }

    public void add_neighbor(int node, int out_port, int in_port){
            this.in_ports.put(node, in_port);
            this.out_ports.put(node, out_port);
    }

    public void open_servers(){
        for (int node : this.in_ports.keySet()){
            try{
                ServerSocket server = new ServerSocket(this.in_ports.get(node));
                this.servers.put(node, server);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void open_sockets(){
        try {
            for (int node : this.out_ports.keySet()) {
                Socket socket = new Socket("localhost", this.out_ports.get(node));
                this.sockets.put(node, socket);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                this.out_data.put(node, out);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void connect_servers(){
        try {
            for (int node : this.out_ports.keySet()) {
                Socket socket = this.servers.get(node).accept();
                DataInputStream in = new DataInputStream(socket.getInputStream());
                this.in_data.put(node, in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void kill(){
        try {
            for (int node_key : this.in_data.keySet()) {
                this.out_data.get(node_key).close();
                this.in_data.get(node_key).close();
                this.sockets.get(node_key).close();
                this.servers.get(node_key).close();
            }
            this.out_data.clear();
            this.in_data.clear();
            this.sockets.clear();
            this.servers.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void print_graph() {
        for(int i = 0;i < this.num_of_nodes;i ++){
            System.out.print(this.graph_weights[i][0]);
            for(int j = 1;j < this.num_of_nodes;j ++){
                System.out.print(", ");
                System.out.print(this.graph_weights[i][j]);
            }
            System.out.print("\n");
        }
    }
}

class Message{
    private Pair<Pair<Pair<Integer, Integer>, Pair<Double, Integer>>, Pair<Pair<Integer, Integer>, Integer>> message;
    public Message(int node1, int node2, double weight, int turns_to_go, int time, int max_time, int node_id){
        Pair<Integer, Integer> edge = new Pair<>(node1, node2);
        Pair<Double, Integer> message_content = new Pair<>(weight, turns_to_go);
        Pair<Integer, Integer> time_lim = new Pair<>(time, max_time);
        Pair<Pair<Integer, Integer>, Pair<Double, Integer>> first_part = new Pair<>(edge, message_content);
        Pair<Pair<Integer, Integer>, Integer> second_part = new Pair<>(time_lim, node_id);
        this.message = new Pair<>(first_part, second_part);
    }

    public Message(String str){
        ArrayList<String> numbers = new ArrayList<>();
        int index = 0;
        String curr_str = "";
        while(index < str.length()){
            if (str.charAt(index) == '(' || str.charAt(index) == ')' || str.charAt(index) == ',' || str.charAt(index) == ' '){
                if(curr_str.length() > 0) {
                    numbers.add(curr_str);
                    curr_str = "";
                }
                index ++;
                continue;
            }
            curr_str += str.charAt(index);
            index ++;
        }
        Pair<Integer, Integer> edge = new Pair<>(Integer.parseInt(numbers.get(0)), Integer.parseInt(numbers.get(1)));
        Pair<Double, Integer> message_content = new Pair<>(Double.parseDouble(numbers.get(2)), Integer.parseInt(numbers.get(3)));
        Pair<Integer, Integer> time_lim = new Pair<>(Integer.parseInt(numbers.get(4)), Integer.parseInt(numbers.get(5)));
        Pair<Pair<Integer, Integer>, Pair<Double, Integer>> first_part = new Pair<>(edge, message_content);
        Pair<Pair<Integer, Integer>, Integer> second_part = new Pair<>(time_lim, Integer.parseInt(numbers.get(6)));
        this.message = new Pair<>(first_part, second_part);
    }

    public Pair<Integer, Integer> get_edge(){
        return this.message.getKey().getKey();
    }

    public double get_weight(){
        return this.message.getKey().getValue().getKey();
    }

    public int get_time_sent(){
        return this.message.getValue().getKey().getKey();
    }

    public int get_max_time(){
        return this.message.getValue().getKey().getValue();
    }

    public int get_node_id(){
        return this.message.getValue().getValue();
    }

    public void set_max_time(int max_time){
        this.message.getValue().getKey().setValue(max_time);
    }

    @Override
    public String toString() {
        return this.message.toString();
    }
}
