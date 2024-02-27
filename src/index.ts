import dayjs, { Dayjs } from "dayjs";
import axios, { AxiosError } from "axios";
import { writeFile } from "fs/promises";
import pg from "pg";

require("dotenv").config();

interface TimeRange {
  from: Dayjs;
  to: Dayjs;
}

interface GraylogQueryResponse {
  messages: {
    message: {
      client_addr: string;
      client_addr_city_name: string;
      client_addr_country_code: string;
      client_addr_geolocation: string;
      http_cf_connecting_ip: string;
      http_cf_connecting_ip_city_name: string;
      http_cf_connecting_ip_country_code: string;
      http_cf_connecting_ip_geolocation: string;
      http_user_agent: string;
      timestamp: string;
    };
  }[];
}

async function getGraylogLogs(posts: { id: string; range: TimeRange }[]) {
  const query = (post: { id: string; range: TimeRange }) =>
    axios.get<GraylogQueryResponse>(
      `${process.env.GRAYLOG_API_URL}/search/universal/absolute`,
      {
        params: {
          query: `http_request_uri_normalized:"/publish/publishing/workflows/${post.id}/publish"`,
          from: post.range.from.format("YYYY-MM-DD HH:mm:ss"),
          to: post.range.to.format("YYYY-MM-DD HH:mm:ss"),
        },
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          Authorization:
            "Basic " +
            btoa(
              `${process.env.GRAYLOG_USER_ID}:${process.env.GRAYLOG_PASSWORD}`
            ),
        },
      }
    );

  const data: Record<string, any> = {};

  for (const post of posts) {
    const response = await query(post);

    if (response.data.messages.length > 1) {
      console.error(post.id, "Detected more than one /publish log");
      continue;
    }

    if (response.data.messages.length === 0) {
      console.error(
        post.id,
        "Detected no /publish log (Probably this post was scheduled)"
      );
      continue;
    }

    data[post.id] = response.data.messages[0].message;
  }

  return data;
}

const query = `
select id, posteddate from publication 
	where orgid='104576'
	and createddate::date >= '2024-02-01'::date
	and posteddate is not null
	order by createddate desc
`;

async function queryDataFromMasterDb<T extends pg.QueryResultRow>(
  query: string
) {
  const client = new pg.Client({
    user: process.env.MASTERDB_USER,
    password: process.env.MASTERDB_PASSWORD,
    database: process.env.MASTERDB_DATABASE,
    port: Number.parseInt(process.env.MASTERDB_PORT!),
    host: process.env.MASTERDB_HOST,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  await client.connect();

  return client.query<T>(query);
}

async function main() {
  const response = await queryDataFromMasterDb<{
    id: string;
    posteddate: Date;
  }>(query);

  const logs = await getGraylogLogs(
    response.rows.map((row) => ({
      id: row.id,
      range: {
        from: dayjs(row.posteddate).subtract(12, "hours"),
        to: dayjs(row.posteddate).add(12, "hours"),
      },
    }))
  );

  await writeFile("scan.lexus2.json", JSON.stringify(logs));
}

main();
