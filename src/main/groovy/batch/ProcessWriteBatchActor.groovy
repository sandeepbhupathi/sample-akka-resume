package batch

import akka.actor.UntypedActor
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.querybuilder.Batch
import com.datastax.driver.core.querybuilder.QueryBuilder
import dao.CassandraSession
import vo.FolderInfo

import java.sql.SQLException

/**
 * Created by A1333177 on may 25th 2017
 */
class ProcessWriteBatchActor extends UntypedActor {

    @Override
    void onReceive(Object message) throws Exception {
        if (message instanceof FolderInfo) {
            //process the item and write it
            FolderInfo folderInfo = (FolderInfo) message
           // int lastGenerated = Integer.valueOf(message.toString())
            List<String> queryList = new ArrayList<String>()
            StringBuffer sBuffer
            int rowsCount = folderInfo.getRequested_Qty()
            for(int i=0; i<rowsCount;i++)
            {
                    sBuffer = new StringBuffer("INSERT INTO Claimable (Claimable_id, parent_folder, status) VALUES (")
                    sBuffer.append(i)
                    sBuffer.append(",")
                    sBuffer.append("'")
                    sBuffer.append(folderInfo.folder_ID)
                    sBuffer.append("'")
                    sBuffer.append(",")
                    sBuffer.append("1")
                    sBuffer.append(")")
                    queryList.add(sBuffer.toString())

            }
            write(queryList)
        } else {
            unhandled(message)
        }
    }



    private  static void write(List<String> sqlQuery) throws SQLException {
        Batch batch = QueryBuilder.unloggedBatch()
        Session session = CassandraSession.getCassandraSession()
        //System.out.println(sqlQuery.size())
        int count =1
        SimpleStatement simpleStatement
        try
        {
            for (String query : sqlQuery) {
                simpleStatement = new SimpleStatement(query)

                batch.add(simpleStatement)
                if(count % 500 == 0 || count == sqlQuery.size())
                {
                    session.execute(batch)
                    batch = QueryBuilder.unloggedBatch()
                }
                count ++
            }
            System.out.print(count)
            if(count <= 500)
            {
                session.execute(batch)
            }
        }
        catch(Exception e) {
            e.printStackTrace()
        }


    }


}
