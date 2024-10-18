const axios = require("axios");
const dotenv = require("dotenv");

dotenv.config();

const METABASE_URL = process.env.METABASE_URL;
const METABASE_USER = process.env.METABASE_USER;
const METABASE_PASSWORD = process.env.METABASE_PASSWORD;

const auth = {
  username: METABASE_USER,
  password: METABASE_PASSWORD,
};

const headers = {
  Authorization: `Basic ${Buffer.from(
    `${METABASE_USER}:${METABASE_PASSWORD}`
  ).toString("base64")}`,
};

// Get the complete list of questions
const questionsResponse = axios.get(`${METABASE_URL}/api/card`, {
  headers: headers,
});

// Filter for public questions
const questions = questionsResponse.data.filter((q) => q.public_uuid);
console.log(
  `${questions.length} public of ${questionsResponse.data.length} questions`
);
