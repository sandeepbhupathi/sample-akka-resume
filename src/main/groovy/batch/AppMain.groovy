package batch


import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.routing.RoundRobinRouter
import com.typesafe.config.ConfigFactory
import groovy.transform.Canonical

@Canonical
class FolderInfo {
    String folder_ID
    int requested_Qty
    String promotion_id

    
}

def processCreateCoupons(FolderInfo folderInfo)
{
    ActorSystem _system = ActorSystem.create("balancing-dispatcher",
            ConfigFactory.load("dispatcher").getConfig("MyDispatcherExample"))

    ActorRef actor = _system.actorOf(new Props(ProcessWriteBatchActor)
            .withDispatcher("balancingDispatcher1").withRouter(
            new RoundRobinRouter(1)))

    if (folderInfo!= null) {
        actor.tell(folderInfo,actor)
    }
    System.out.println(actor.terminated)
    _system.awaitTermination()
}



def FolderInfo getRequestData()
{
    FolderInfo fi = new FolderInfo()
    fi.setFolder_ID("test")
    fi.setPromotion_id("42268")
    fi.setRequested_Qty(5000)
    fi
}


System.out.print(System.currentTimeMillis() % 1000)
processCreateCoupons(getRequestData())
System.out.print(System.currentTimeMillis() % 1000)

