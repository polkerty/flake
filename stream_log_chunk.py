#!/usr/bin/env python3

import os
import json
import argparse
import getpass

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

def fetch_and_chunk_logs(conninfo_or_params, lookback, max_chars):
    """
    Use a named (server-side) cursor to stream rows from Postgres,
    parse each log, and return JSON of chunked results.
    """
    # If we got a string, treat it as conninfo; if we got a dict, unpack it.
    if isinstance(conninfo_or_params, str):
        conn = psycopg2.connect(conninfo_or_params)
    else:
        conn = psycopg2.connect(**conninfo_or_params)

    results = []
    with conn.cursor(name="log_stream_cursor", cursor_factory=DictCursor) as cur:
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
                # Keep only the last `max_chars` characters
                text_section = text_section[-max_chars:]

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
        description="Fetch logs from Postgres, chunk by MAGIC delimiter, output JSON.",
        add_help=False
    )

    # Then add a custom help flag under something else, e.g. `-H/--help`
    parser.add_argument(
        "-H", "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit"
    )


    # psql-style short options for connection
    parser.add_argument("-h", "--host", default=os.getenv("PGHOST", "localhost"),
                        help="Database server host. Defaults from $PGHOST or 'localhost'.")
    parser.add_argument("-p", "--port", default=os.getenv("PGPORT", 5432), type=int,
                        help="Database server port. Defaults from $PGPORT or 5432.")
    parser.add_argument("-d", "--dbname", default=os.getenv("PGDATABASE", "postgres"),
                        help="Database name. Defaults from $PGDATABASE or 'postgres'.")
    parser.add_argument("-U", "--user", default=os.getenv("PGUSER", "postgres"),
                        help="Database user. Defaults from $PGUSER or 'postgres'.")

    # The following two aren’t exactly 1:1 with psql, but approximate the idea:
    parser.add_argument("-w", "--no-password", action="store_true",
                        help="Never prompt for password. (Sets an empty password.)")
    parser.add_argument("-W", "--password", action="store_true",
                        help="Force password prompt. (Will ignore $PGPASSWORD if set.)")

    # We also allow a direct connection string if desired.
    parser.add_argument("--conninfo",
                        help="Full libpq connection string, e.g. 'host=... port=... dbname=... user=...'")

    # Additional query/filter parameters
    parser.add_argument("--lookback", default="6 months",
                        help="Lookback period recognized by PostgreSQL interval syntax "
                             "(e.g. '2 days', '3 weeks', '1 year'). Default: '6 months'.")
    parser.add_argument("--max-chars", default=1000, type=int,
                        help="Number of characters to include from the end of each chunk. Default: 1000.")

    args = parser.parse_args()

    # Determine password
    # 1) If --conninfo is provided, we'll not parse host/port/dbname/user.
    # 2) Else build a conn_params dict. We approximate psql's approach for -w/-W.
    if args.conninfo:
        # They gave a direct connection string - just use it.
        conninfo_or_params = args.conninfo
        # If user also did -W, we can force a prompt for password
        if args.password:
            pw = getpass.getpass("Password: ")
            # Append or override password=... in the conninfo
            conninfo_or_params += f" password='{pw}'"
        elif args.no_password:
            # Force empty password in conninfo
            conninfo_or_params += " password=''"
    else:
        # Build connection parameters
        pg_password_env = os.getenv("PGPASSWORD", "")
        if args.password:
            # Force prompt
            pg_password = getpass.getpass("Password: ")
        elif args.no_password:
            # Force empty
            pg_password = ""
        else:
            pg_password = pg_password_env

        conninfo_or_params = {
            "host": args.host,
            "port": args.port,
            "dbname": args.dbname,
            "user": args.user,
            "password": pg_password
        }

    json_output = fetch_and_chunk_logs(conninfo_or_params, args.lookback, args.max_chars)
    print(json_output)

if __name__ == "__main__":
    main()
