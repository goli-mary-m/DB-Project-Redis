package com.company;

import redis.clients.jedis.Jedis;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws FileNotFoundException {

        Jedis jedis = new Jedis();

        String[] currentLine;
        Scanner fileScanner = new Scanner(new File("NYSE_20210301.csv"));
        while (fileScanner.hasNext()){
            currentLine = fileScanner.next().split(",");
            jedis.set(currentLine[0], currentLine[1]);
        }
        fileScanner.close();

        Scanner inputScanner = new Scanner(System.in);
        String statement, key, value;
        while (true){
            statement = inputScanner.next();
            if(statement.equals("create")){   // Create -> create <key> <value>
                key = inputScanner.next();
                value = inputScanner.next();
                if(!jedis.exists(key)){
                    jedis.set(key, value);
                    System.out.print("true");
                }else{
                    System.out.print("false");
                }
                System.out.println("\n");
            }else if(statement.equals("fetch")){  // Fetch -> fetch <key>
                key = inputScanner.next();
                if(!jedis.exists(key)){
                    System.out.print("false");
                }else{
                    System.out.println("true");
                    value = jedis.get(key);
                    System.out.print(value);
                }
                System.out.println("\n");
            }else if(statement.equals("update")){  //Update -> update <key> <value>
                key = inputScanner.next();
                value = inputScanner.next();
                if(!jedis.exists(key)){
                    System.out.print("false");
                }else{
                    System.out.print("true");
                    jedis.del(key);
                    jedis.set(key, value);
                }
                System.out.println("\n");
            }else if(statement.equals("delete")){  //Delete -> delete <key>
                key = inputScanner.next();
                if(!jedis.exists(key)){
                    System.out.print("false");
                }else{
                    System.out.print("true");
                    jedis.del(key);
                }
                System.out.println("\n");
            }else{
                System.out.println("invalid statement!\n");
            }
        }
    }
}
