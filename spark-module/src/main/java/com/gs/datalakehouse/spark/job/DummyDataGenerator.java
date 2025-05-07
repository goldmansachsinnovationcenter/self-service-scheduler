package com.gs.datalakehouse.spark.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Spark job to generate dummy data for the data lakehouse.
 */
public class DummyDataGenerator {

    private static final Random random = new Random();

    public static void main(String[] args) {
        String table = "customers";
        int count = 1000;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--table") && i + 1 < args.length) {
                table = args[i + 1];
                i++;
            } else if (args[i].equals("--count") && i + 1 < args.length) {
                count = Integer.parseInt(args[i + 1]);
                i++;
            }
        }

        SparkSession spark = SparkSession.builder()
                .appName("DummyDataGenerator")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs:///warehouse")
                .getOrCreate();

        Dataset<Row> data;
        switch (table) {
            case "customers":
                data = generateCustomerData(spark, count);
                break;
            case "transactions":
                data = generateTransactionData(spark, count);
                break;
            case "products":
                data = generateProductData(spark, count);
                break;
            default:
                throw new IllegalArgumentException("Unknown table: " + table);
        }

        data.write()
                .format("iceberg")
                .mode("append")
                .save("spark_catalog.default." + table);

        spark.stop();
    }

    /**
     * Generates dummy customer data.
     */
    private static Dataset<Row> generateCustomerData(SparkSession spark, int count) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("customer_id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("phone", DataTypes.StringType, true),
                DataTypes.createStructField("address", DataTypes.StringType, true),
                DataTypes.createStructField("registration_date", DataTypes.TimestampType, false)
        });

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = "C" + (10000 + i);
            String name = getRandomName();
            String email = name.toLowerCase().replace(" ", ".") + "@example.com";
            String phone = getRandomPhone();
            String address = getRandomAddress();
            Timestamp registrationDate = getRandomTimestamp(365 * 2); // Last 2 years

            rows.add(org.apache.spark.sql.RowFactory.create(
                    id, name, email, phone, address, registrationDate
            ));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates dummy transaction data.
     */
    private static Dataset<Row> generateTransactionData(SparkSession spark, int count) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("transaction_id", DataTypes.StringType, false),
                DataTypes.createStructField("customer_id", DataTypes.StringType, false),
                DataTypes.createStructField("product_id", DataTypes.StringType, false),
                DataTypes.createStructField("transaction_date", DataTypes.TimestampType, false),
                DataTypes.createStructField("amount", DataTypes.DoubleType, false),
                DataTypes.createStructField("payment_method", DataTypes.StringType, true)
        });

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = "T" + (100000 + i);
            String customerId = "C" + (10000 + random.nextInt(1000));
            String productId = "P" + (1000 + random.nextInt(200));
            Timestamp transactionDate = getRandomTimestamp(180); // Last 6 months
            double amount = 10.0 + random.nextDouble() * 990.0; // $10 to $1000
            String paymentMethod = getRandomPaymentMethod();

            rows.add(org.apache.spark.sql.RowFactory.create(
                    id, customerId, productId, transactionDate, amount, paymentMethod
            ));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates dummy product data.
     */
    private static Dataset<Row> generateProductData(SparkSession spark, int count) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("product_id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("category", DataTypes.StringType, true),
                DataTypes.createStructField("price", DataTypes.DoubleType, false),
                DataTypes.createStructField("inventory", DataTypes.IntegerType, true),
                DataTypes.createStructField("last_updated", DataTypes.TimestampType, false)
        });

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = "P" + (1000 + i);
            String name = getRandomProductName();
            String category = getRandomProductCategory();
            double price = 5.0 + random.nextDouble() * 495.0; // $5 to $500
            int inventory = random.nextInt(1000);
            Timestamp lastUpdated = getRandomTimestamp(30); // Last month

            rows.add(org.apache.spark.sql.RowFactory.create(
                    id, name, category, price, inventory, lastUpdated
            ));
        }

        return spark.createDataFrame(rows, schema);
    }


    private static String getRandomName() {
        String[] firstNames = {"John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Jennifer", "William", "Elizabeth"};
        String[] lastNames = {"Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"};
        return firstNames[random.nextInt(firstNames.length)] + " " + lastNames[random.nextInt(lastNames.length)];
    }

    private static String getRandomPhone() {
        return String.format("(%03d) %03d-%04d", 
                100 + random.nextInt(900), 
                100 + random.nextInt(900), 
                1000 + random.nextInt(9000));
    }

    private static String getRandomAddress() {
        String[] streets = {"Main St", "Oak Ave", "Maple Rd", "Cedar Ln", "Pine Dr"};
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"};
        String[] states = {"NY", "CA", "IL", "TX", "AZ"};
        
        int number = 100 + random.nextInt(9900);
        String street = streets[random.nextInt(streets.length)];
        String city = cities[random.nextInt(cities.length)];
        String state = states[random.nextInt(states.length)];
        String zip = String.format("%05d", 10000 + random.nextInt(90000));
        
        return number + " " + street + ", " + city + ", " + state + " " + zip;
    }

    private static Timestamp getRandomTimestamp(int daysBack) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime randomDate = now.minusDays(random.nextInt(daysBack));
        return Timestamp.valueOf(randomDate);
    }

    private static String getRandomPaymentMethod() {
        String[] methods = {"Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"};
        return methods[random.nextInt(methods.length)];
    }

    private static String getRandomProductName() {
        String[] adjectives = {"Premium", "Deluxe", "Basic", "Advanced", "Professional"};
        String[] products = {"Laptop", "Smartphone", "Headphones", "Monitor", "Keyboard", "Mouse", "Tablet", "Camera"};
        return adjectives[random.nextInt(adjectives.length)] + " " + products[random.nextInt(products.length)];
    }

    private static String getRandomProductCategory() {
        String[] categories = {"Electronics", "Computers", "Audio", "Accessories", "Photography"};
        return categories[random.nextInt(categories.length)];
    }
}
