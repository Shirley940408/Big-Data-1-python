# NASA_access_log_Jul95-part.gz
# host             datetime                      path                        status bytes
# 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
# unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
# 199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085
# burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0
# 199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179
# burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0
# burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0
# 205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985
# d104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
# 129.94.144.152 - - [01/Jul/1995:00:00:13 -0400] "GET / HTTP/1.0" 200 7074
import gzip
import os
import re
import sys
import uuid
from datetime import datetime, timezone, timedelta
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType


def get_hostname_datatime_path_bytes(line):
    pattern = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = pattern.match(line)
    if match:
        return tuple(match.group(i) for i in range (1,5)) # hostname, datetimeString, path, numbers_of_bytes_String

def main():
    input_dir = sys.argv[1]
    namespace = sys.argv[2]
    table_name = sys.argv[3]
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(namespace)
    batch = BatchStatement(batch_type=BatchType.UNLOGGED) #UNLOGGED for Non-atomic batch operation.

    # Prepare the INSERT statement
    prepared_statement = session.prepare(
        f"INSERT INTO {table_name} (host, datetime, path, bytes, req_id) VALUES (?, ?, ?, ?, ?)"
    )
    BATCH_SIZE = 300
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            count = 0
            for line in logfile:
                hostname, datetimeString, path, numbers_of_bytes_String = get_hostname_datatime_path_bytes(line)
                # format timeString to timestamp
                naive = datetime.strptime(datetimeString, "%d/%b/%Y:%H:%M:%S")
                bound_statement = prepared_statement.bind((hostname, naive, path, int(numbers_of_bytes_String), uuid.uuid1()))
                batch.add(bound_statement)
                count += 1
                if count == BATCH_SIZE:
                    session.execute(batch)
                    batch.clear()
                    count = 0
            session.execute(batch)
            batch.clear()

if __name__ == "__main__":
    main()