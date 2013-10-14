import com.mongodb.*;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 *
 * @author Chenxi "Chauncey" Wang
 * @version 10/3/13
 */
public class Circle {
    private DBCollection table = null;

    public static void main(String[] args) {
        (new Circle()).runAnalysis();

    }

    /**
     * set up database and process input
     *
     */

    private void runAnalysis() {
        MongoClient mongo = null;
        DB db = null;
        try {
            mongo = new MongoClient("localhost", 27017);
            db = mongo.getDB("circle");
            table = db.getCollection("users");
            table.ensureIndex(new BasicDBObject("loc", "2d"), "geospacialIdx");
            table.ensureIndex(new BasicDBObject("user_id", 1));

            chooseAndProcessFile();


        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {

            //clean up the database
            table.drop();

        }

    }


    /**
     * Insert Operations
     *
     * @param table Database Collection
     * @param command Insert Query
     */

    public  void insert(DBCollection table, String command) {
        String[] parts = command.trim().split("\\s+");
        int len = parts.length;
        if (len < 4) return;
        Map<String, Object> documentMap = new HashMap<String, Object>();
        documentMap.put("user_id", Long.parseLong(parts[1]));
        documentMap.put("loc", new double[]{Double.parseDouble(parts[3]), Double.parseDouble(parts[2])});
        Map<String, Object> interestMap = new HashMap<String, Object>();

        for (int i = 4; i < len; i = i + 2) {
            interestMap.put(parts[i], Double.parseDouble(parts[i + 1]));
        }
        documentMap.put("interests", interestMap);
        table.insert(new BasicDBObject(documentMap));


    }

    /**
     * Query Operations
     *
     * @param table  Databes Collection
     * @param command Query
     * @return Query Result
     */

    public  String query(DBCollection table, String command) {
        String[] parts = command.trim().split("\\s+");
        int len = parts.length;
        if (len < 3) return "Invalid Query! Please check your query statement. ";
        long userID = Long.parseLong(parts[1]);
        int distance = Integer.parseInt(parts[2]);
        DBObject user = (DBObject) table.findOne(new BasicDBObject("user_id", userID));

        if (user == null) return "Invalid Query! This user does not exist. ";

        BasicDBObject inter = (BasicDBObject) user.get("interests");
        HashMap<String, Double> interestMap = (HashMap<String, Double>) inter.toMap();

        DBObject location = (DBObject) user.get("loc");

        BasicDBObject query = new BasicDBObject("loc", new BasicDBObject().append("$nearSphere", location).append("$maxDistance", (double) distance / 3959));

        PriorityQueue<QueryResult> topList = new PriorityQueue<QueryResult>(11, new Comparator<QueryResult>() {
            @Override
            public int compare(QueryResult q1, QueryResult q2) {
                if (q1.getScalaProduct() >q2.getScalaProduct() ) return -1;
                else if (q1.getScalaProduct() < q2.getScalaProduct()) return 1;
                else {
                    if (q1.getUserId() > q2.getUserId()) return 1;
                    else if (q1.getUserId() < q2.getUserId()) return -1;
                    else return 0;
                }
            }
        });

        for (final DBObject usr : table.find(query).toArray()) {
//            System.out.println(usr);
            BasicDBObject tmp = new BasicDBObject((Map) usr);
            Long id = tmp.getLong("user_id");
            HashMap<String, Double> tmpInterestMap = (HashMap<String, Double>) ((BasicDBObject) tmp.get("interests")).toMap();
            double scalaProduct = getCommonInterests(interestMap, tmpInterestMap);
            topList.add(new QueryResult(id, scalaProduct));
        }

        return getTop10List(topList,userID);
    }

    /**
     * Generate the query results
     *
     *
     * @param topList MaxHeap
     * @param userID
     * @return
     */
    private  String getTop10List(PriorityQueue<QueryResult> topList, long userID) {
        StringBuilder sb = new StringBuilder();
        while (!topList.isEmpty()) {
            QueryResult current = topList.remove();
//            System.out.println(current.getUserId() + " " + current.getScalaProduct());
            if (current.getUserId() == userID) continue;
            sb.append(current.getUserId()).append(" ");

        }

        return sb.toString();

    }

    /**
     * Compute common interest scalar product
     *
     * @param interestMap user of target
     * @param tmpInterestMap usrs within targeted miles of distance
     * @return
     */

    private  double getCommonInterests(HashMap<String, Double> interestMap, HashMap<String, Double> tmpInterestMap) {
        double product = 0;
        for (String interest : interestMap.keySet()) {
            if (tmpInterestMap.containsKey(interest)) {
                product += interestMap.get(interest) * tmpInterestMap.get(interest);
            }
        }

        return product;
    }

    /**
     * inner class to hold scalaProduct and userId
     *
     *
     */

    private class QueryResult {
        private long userId;
        private double scalaProduct;

        private QueryResult(long userId, double scalaProduct) {
            this.userId = userId;
            this.scalaProduct = scalaProduct;
        }

        private long getUserId() {
            return userId;
        }

        private double getScalaProduct() {
            return scalaProduct;
        }
    }



    /**
     * Read inupt files and do operations based on each line's content
     *
     */
    public void process(String fileName) {

        BufferedReader reader = null;

//      fileName = "/Users/lrgfc/data";
//      fileName = "/Users/lrgfc/CircleTest.txt";
        try {

            reader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        String line = null;

        int line_no = 0;
        int totalLines = -1;

        if (reader != null) {
            try {
                while ((line = reader.readLine()) != null) {
//                    System.out.println(line);
                    if (line_no == 0) {
                        totalLines = Integer.parseInt(line.trim());
                    } else if(line_no == totalLines + 1) {
                        return;
                    }
                    else {
                        if (line.startsWith("W")) {
                            insert(table,line);
                        } else if (line.startsWith("Q")) {
                            if (line_no == 1) {
                                System.out.println();

                            } else {
                                System.out.println(query(table,line));
                            }

                        } else {

                        }

                    }

                    line_no++;


                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

    }

    private void chooseAndProcessFile() {
        // The creation of Swing components must be done in Event Dispatcher Thread
        // otherwise, sometimes the JFileChooser dialog will not be shown
        try {
            EventQueue.invokeAndWait(new Runnable() {
                @Override
                public void run() {
                    try {
                        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
                    } catch (Exception ignored) {
                    }
                    JFileChooser fileChooser = new JFileChooser();
                    fileChooser.setCurrentDirectory(new File("."));
                    int state = fileChooser.showOpenDialog(null);
                    if (state == JFileChooser.APPROVE_OPTION) {
                        File file = fileChooser.getSelectedFile();
                        try {
                            if (file != null) {
                                String fileName = file.getCanonicalPath();
                                process(fileName);


                            }
                        } catch (IOException e) {
                        }
                    }
                }
            });
        } catch (InterruptedException | InvocationTargetException ignored) {
        }
    }
}