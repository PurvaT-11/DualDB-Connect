const express = require("express");
const { MongoClient, ObjectId } = require("mongodb");
const mysql = require("mysql2/promise");

const app = express();
app.use(express.json());

// -------------------------- MongoDB Configurations --------------------------
const mongoUri =
  "mongodb+srv://ptand:ptand@cluster0.sjhqu.mongodb.net/?retryWrites=true&w=majority";
const mongoClient = new MongoClient(mongoUri);

let dbs = {}; 

// Connect to MongoDB
(async () => {
  try {
    await mongoClient.connect();
    dbs.sample_mflix = mongoClient.db("sample_mflix");
    dbs.cs480_project2 = mongoClient.db("cs480-project2");
    console.log("Connected to MongoDB: sample_mflix and cs480_project2");

    // Start server only after successful connection
    const PORT = 3002;
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("MongoDB connection error:", error);
    process.exit(1); // Exit if MongoDB connection fails
  }
})();

// -------------------------- MySQL Configurations --------------------------
const mysqlPool = mysql.createPool({
  host: "34.133.217.195",
  user: "dhayes1",
  password: "ptan8790",
  database: "sakila",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// -------------------------- MongoDB Error Handler --------------------------
const handleMongoError = (res, error) => {
  console.error("MongoDB Error:", error);
  res.status(500).json(["An error has occurred."]);
};

// -------------------------- MySQL Error Handler --------------------------
const handleMySQLError = (res, error) => {
  console.error("MySQL Error:", error);
  res.status(500).json(["An error has occurred."]);
};

// -------------------------- MongoDB Routes --------------------------

// Root route
app.get("/", (req, res) => {
  res.send("Welcome to the API!");
});

// GET: Return all colors from MongoDB
app.get("/api/v1/colors", async (req, res) => {
  try {
    if (!dbs.cs480_project2) {
      return res.status(500).json(["MongoDB connection is not established."]);
    }
    const colors = await dbs.cs480_project2.collection("colors").find().toArray();
    res.json(colors);
  } catch (error) {
    handleMongoError(res, error);
  }
});
// GET /movies endpoint
app.get('/api/v1/movies', async (req, res) => {
  try {
    // Ensure the connection to the sample_mflix database is established
    if (!dbs.sample_mflix) {
      return res.status(500).json(["MongoDB connection is not established."]);
    }

    const query = {};

    // Dynamically build query based on provided query parameters
    if (req.query.genre) {
      query.genres = req.query.genre; // Filter by genre
    }
    if (req.query.year) {
      query.year = parseInt(req.query.year, 10); // Filter by year
    }
    if (req.query.director) {
      query.directors = { $regex: new RegExp(req.query.director, 'i') }; // Case-insensitive match for director
    }

    // Fetch documents from the movies collection with a limit of 10
    const movies = await dbs.sample_mflix.collection('movies').find(query).limit(10).toArray();

    res.json(movies); // Send the fetched documents as JSON
  } catch (error) {
    console.error(error);
    res.status(500).json(["An error has occurred."]); // Return error response if something goes wrong
  }
});

// -------------------------- MySQL Routes --------------------------

// GET: All films from MySQL (optional search query)
app.get("/api/v1/films", async (req, res) => {
  try {
    let query = "SELECT * FROM film";
    const params = [];

    // If query parameter is provided, perform case-insensitive search on title
    if (req.query.query) {
      query += " WHERE title LIKE ?";
      params.push(`%${req.query.query}%`);
    }

    const [rows] = await mysqlPool.query(query, params);
    res.json(rows); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Film by ID from MySQL (wrapped in array)
app.get("/api/v1/films/:id", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      "SELECT * FROM film WHERE film_id = ?",
      [req.params.id]
    );
    res.json([rows[0]] || []); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: All actors from MySQL
app.get("/api/v1/actors", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query("SELECT * FROM actor");
    res.json(rows);
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Actor by ID from MySQL (wrapped in array)
app.get("/api/v1/actors/:id", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      "SELECT * FROM actor WHERE actor_id = ?",
      [req.params.id]
    );
    res.json([rows[0]] || []); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: All customers from MySQL
app.get("/api/v1/customers", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query("SELECT * FROM customer");
    res.json(rows);
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Customer by ID from MySQL
app.get("/api/v1/customers/:id", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      "SELECT * FROM customer WHERE customer_id = ?",
      [req.params.id]
    );
    res.json([rows[0]] || []); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: All stores from MySQL
app.get("/api/v1/stores", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query("SELECT * FROM store");
    res.json(rows);
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Store by ID from MySQL
app.get("/api/v1/stores/:id", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      "SELECT * FROM store WHERE store_id = ?",
      [req.params.id]
    );
    res.json([rows[0]] || []); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Films for an actor from MySQL
app.get("/api/v1/actors/:id/films", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      `SELECT f.* FROM film f
       INNER JOIN film_actor fa ON f.film_id = fa.film_id
       WHERE fa.actor_id = ?`,
      [req.params.id]
    );
    res.json(rows);
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Actors in a film from MySQL
app.get("/api/v1/films/:id/actors", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      `SELECT a.* FROM actor a
       INNER JOIN film_actor fa ON a.actor_id = fa.actor_id
       WHERE fa.film_id = ?`,
      [req.params.id]
    );
    res.json(rows);
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Film details from `film_list` view by ID from MySQL
app.get("/api/v1/films/:id/detail", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      "SELECT * FROM film_list WHERE FID = ?",
      [req.params.id]
    );
    res.json([rows[0]] || []); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Customer details from `customer_list` view by ID from MySQL
app.get("/api/v1/customers/:id/detail", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      "SELECT * FROM customer_list WHERE ID = ?",
      [req.params.id]
    );
    res.json([rows[0]] || []); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Actor info from `actor_info` view by ID from MySQL
app.get("/api/v1/actors/:id/detail", async (req, res) => {
  try {
    const [rows] = await mysqlPool.query(
      "SELECT * FROM actor_info WHERE actor_id = ?",
      [req.params.id]
    );
    res.json([rows[0]] || []); // Always return as an array
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// GET: Inventory in stock for a film at a store from MySQL
app.get("/api/v1/inventory-in-stock/:film_id/:store_id", async (req, res) => {
  try {
    const [results] = await mysqlPool.query(
      "CALL film_in_stock(?, ?, @count)",
      [req.params.film_id, req.params.store_id]
    );
    res.json(results[0] || []);
  } catch (error) {
    handleMySQLError(res, error);
  }
});

// -------------------------- Start Server --------------------------
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
