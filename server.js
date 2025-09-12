import 'dotenv/config';
import express from "express";
import helmet from "helmet";
import cors from "cors";
import pkg from "pg";

const { Pool } = pkg;

// ENV
const PORT = process.env.PORT || 10000;
// Put your Render DATABASE_URL in Render env later; for local dev use .env file
const DATABASE_URL = process.env.DATABASE_URL;
const API_KEY = process.env.API_KEY;

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL. Set in .env for local dev and in Render env for prod.");
  process.exit(1);
}
if (!API_KEY) {
  console.error("Missing API_KEY. Set in .env for local dev and in Render env for prod.");
  process.exit(1);
}

// DB pool
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false } // Render Postgres requires SSL
});

// App
const app = express();
app.use(helmet());
app.use(cors()); // You can restrict origins if you want
app.use(express.json());

// Health (no API key required)
app.get("/health", (req, res) => {
  res.json({ ok: true, service: "vila-sales-api" });
});

// Simple API key middleware (protects /api/*)
app.use("/api", (req, res, next) => {
  const key = req.header("x-api-key");
  if (!key || key !== API_KEY) return res.status(401).json({ error: "unauthorized" });
  next();
});

/**
 * Adjust the SELECT below to match your Postgres column names.
 * Aliases (AS ...) are the JSON keys returned to your Apps Script.
 * If you DON'T have seller_category or buyer_nipt in DB, you can return '' as those fields, e.g. '' AS seller_category.
 */
const BASE_SELECT = `
  SELECT
    "Order_ID"                AS order_id,
    "Seller"                  AS seller,
    "Article_Name"            AS article_name,
    "Category"                AS category,
    "Quantity"                AS quantity,
    "Total_Article_Price"     AS total_article_price,
    "Datetime"                AS datetime,
    "Seller_Category"         AS seller_category,   -- OR replace with '' if not in DB
    "Buyer_NIPT"              AS buyer_nipt         -- OR replace with '' if not in DB
  FROM sales
`;

// Incremental: everything after a timestamp (exclusive)
app.get("/api/sales/since", async (req, res) => {
  try {
    const since = req.query.since;
    if (!since) return res.status(400).json({ error: "missing 'since' query param (ISO timestamp)" });

    const limit = Math.min(parseInt(req.query.limit || "50000", 10), 100000);

    const sql = `${BASE_SELECT}
      WHERE "Datetime" > $1
      ORDER BY "Datetime" ASC
      LIMIT ${limit}`;

    const { rows } = await pool.query(sql, [since]);
    res.json({ rows });
  } catch (e) {
    console.error("since error:", e);
    res.status(500).json({ error: "internal_error" });
  }
});

// Range: [from, to) (to is exclusive upper bound)
app.get("/api/sales/range", async (req, res) => {
  try {
    const from = req.query.from;
    const to   = req.query.to;
    if (!from || !to) return res.status(400).json({ error: "missing 'from' or 'to' query param (ISO)" });

    const limit = Math.min(parseInt(req.query.limit || "100000", 10), 200000);

    const sql = `${BASE_SELECT}
      WHERE "Datetime" >= $1 AND "Datetime" < $2
      ORDER BY "Datetime" ASC
      LIMIT ${limit}`;

    const { rows } = await pool.query(sql, [from, to]);
    res.json({ rows });
  } catch (e) {
    console.error("range error:", e);
    res.status(500).json({ error: "internal_error" });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
});
