#!/usr/bin/env python3

import os
import json
import argparse

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

MAGIC = "==~_~===-=-===~_~=="

def chunk_log(log_text: str, magic: str = MAGIC):
    """
    Split the log into (filename, text_chunk) pairs.
    The first chunk is labeled "head" (i.e., before the first MAGIC).
    Then each subsequent chunk is preceded by <MAGIC>filename<MAGIC>.
    """
    chunks = []
    current_filename = "head"
    pos = 0

    while True:
        # Find the next occurrence of the magic delimiter
        next_magic = log_text.find(magic, pos)
        if next_magic == -1:
            # No more magic: everything from 'pos' to the end is the last chunk
            text_chunk = log_text[pos:]
            chunks.append((current_filename, text_chunk))
            break
        # The text before 'next_magic' is the chunk for the current filename
        text_chunk = log_text[pos:next_magic]
        chunks.append((current_filename, text_chunk))

        # Move past the magic delimiter
        pos = next_magic + len(magic)

        # Now read until the next magic to grab the "filename"
        next_magic2 = log_text.find(magic, pos)
        if next_magic2 == -1:
            # If we never find the second magic, treat the rest as the filename
            current_filename = log_text[pos:]
            # There's no text chunk after that, so we end
            break
        current_filename = log_text[pos:next_magic2]

        # Move 'pos' past this second magic
        pos = next_magic2 + len(magic)

    return chunks

def fetch_and_chunk_logs(conn_params, lookback):
    """
    Use a named (server-side) cursor to stream rows from Postgres,
    parse each log, and return JSON of chunked results.
    """
    results = []

    # Connect to Postgres
    conn = psycopg2.connect(**conn_params)
    with conn.cursor(name="log_stream_cursor", cursor_factory=DictCursor) as cur:
        # Use parameter binding for the interval
        query = """
            SELECT
                sysname,
                snapshot,
                status,
                stage,
                log,
                branch,
                git_head_ref AS commit
            FROM build_status
            WHERE stage != 'OK'
              AND build_status.report_time IS NOT NULL
              AND snapshot > current_date - %s::interval
            ORDER BY snapshot ASC
        """
        cur.execute(query, (lookback,))

        # Iterate rows *streaming*, not all at once
        for row in cur:
            # row["log"] could be huge
            log_text = row["log"] or ""

            # Break out the log into chunks
            for filename, text_section in chunk_log(log_text, MAGIC):
                # Keep only the last 1000 characters
                text_section = text_section[-1000:]

                results.append({
                    "sysname": row["sysname"],
                    "snapshot": str(row["snapshot"]),  # ensure JSON-friendly
                    "status": row["status"],
                    "stage": row["stage"],
                    "filename": filename,
                    "commit": row["commit"],
                    "branch": row["branch"],
                    "text": text_section
                })

    conn.close()
    return json.dumps(results, indent=2)

def main():
    parser = argparse.ArgumentParser(
        description="Fetch logs from Postgres, chunk by MAGIC delimiter, output JSON."
    )
    # psql-style environment variable defaults
    parser.add_argument("--host", default=os.getenv("PGHOST", "localhost"),
                        help="Database server host. Default from $PGHOST or 'localhost'.")
    parser.add_argument("--port", default=os.getenv("PGPORT", 5432), type=int,
                        help="Database server port. Default from $PGPORT or 5432.")
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", "postgres"),
                        help="Database name. Default from $PGDATABASE or 'postgres'.")
    parser.add_argument("--user", default=os.getenv("PGUSER", "postgres"),
                        help="Database user. Default from $PGUSER or 'postgres'.")
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""),
                        help="Database password. Default from $PGPASSWORD or empty.")
    parser.add_argument("--lookback", default="6 months",
                        help="Lookback period recognized by PostgreSQL interval syntax (e.g. '2 days', '3 weeks', '1 year'). "
                             "Default: '6 months'.")

    args = parser.parse_args()

    # Build connection parameters dictionary
    conn_params = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password
    }

    json_output = fetch_and_chunk_logs(conn_params, args.lookback)
    print(json_output)

if __name__ == "__main__":
    main()
