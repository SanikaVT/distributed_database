package transactionProcessing;
import java.util.List;

import com.dal.distributed.constant.QueryTypes;
import com.dal.distributed.main.Main;
import com.dal.distributed.queryImpl.DeleteDataFromTable;
import com.dal.distributed.queryImpl.InsertIntoTable;
import com.dal.distributed.queryImpl.SelectQuery;
import com.dal.distributed.queryImpl.UpdateTable;
import com.dal.distributed.queryImpl.model.OperationStatus;
import com.dal.distributed.utils.FileOperations;
import com.dal.distributed.utils.Results;

public class TransactionProcessing {
    FileOperations fileOperations=new FileOperations();
    OperationStatus oStatus=null;
    public boolean execute(List<OperationStatus> listTransactionQueries) throws Exception
    {
        Main.isTransaction=true;
        for(int i=0;i<listTransactionQueries.size();i++)
        {
            if(listTransactionQueries.get(i).getQueryType().equals(QueryTypes.DELETE))
            {
                if(listTransactionQueries.get(i).isRepeatTable())
                {
                   oStatus= new DeleteDataFromTable().execute(listTransactionQueries.get(i).getQuery());
                   listTransactionQueries.set(i,oStatus);

                }
                fileOperations.writeDataToPSV(listTransactionQueries.get(i).getResult(), listTransactionQueries.get(i).getFilePath());
            }
            else if(listTransactionQueries.get(i).getQueryType().equals(QueryTypes.UPDATE))
            {
                if(listTransactionQueries.get(i).isRepeatTable())
                {
                    oStatus=new UpdateTable().execute(listTransactionQueries.get(i).getQuery());
                    listTransactionQueries.set(i,oStatus);
                }
                fileOperations.writeDataToPSV(listTransactionQueries.get(i).getResult(), listTransactionQueries.get(i).getFilePath());
            }
            else if(listTransactionQueries.get(i).getQueryType().equals(QueryTypes.INSERT))
            {
                if(listTransactionQueries.get(i).isRepeatTable())
                {
                    oStatus=new InsertIntoTable().execute(listTransactionQueries.get(i).getQuery());
                    listTransactionQueries.set(i,oStatus);
                }
                String finalValue="";
                List<Object> result=listTransactionQueries.get(i).getResult().get(0);
                for(int j=0;j<result.size();j++)
                {
                    finalValue+=result.get(j);
                    if(j!=result.size()-1)
                    finalValue+="|";
                }
                fileOperations.writeStringToPSV(finalValue, listTransactionQueries.get(i).getFilePath());
            }
            else if(listTransactionQueries.get(i).getQueryType().equals(QueryTypes.SELECT))
            {
                if(listTransactionQueries.get(i).isRepeatTable())
                {
                    oStatus=new SelectQuery().execute(listTransactionQueries.get(i).getQuery());
                    listTransactionQueries.set(i,oStatus);
                }
                Results.printResult(listTransactionQueries.get(i).getResult());
            }
        }

        Main.isTransaction=false;
        return true;

    }
}
