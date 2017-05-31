package dao

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session

class CassandraSession {

    public static Session getCassandraSession()
    {
        String serverIp = "127.0.0.1";
        String keyspace = "test";


        Cluster cluster = Cluster.builder()
                .addContactPoints(serverIp)
                .build();

        Session session = cluster.connect(keyspace);

        return session;
    }
}
