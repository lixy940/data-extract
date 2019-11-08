package com.lixy.dataextract.utils;


import com.lixy.dataextract.vo.DbConnInfo;
import com.lixy.dataextract.vo.kettle.KettleCreateInfo;
import org.apache.commons.lang3.StringUtils;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * CreateDate：2019/10/20 <br/>
 * Author：zhubing <br/>
 * Description: the utils for create ETL files
 **/
public  class KettleUtils {

    /**
     * @Description: create kettle files with database is oracle
     * @param createKettleFilesInfo the parameters required by the method
     * @return  kettle files infomation
     */


    public static final int CLOUMN_CATEGORY_DATE = 2;

    public static final int CLOUMN_CATEGORY_DATESTRING = 3;

    public static final int CLOUMN_CATEGORY_OTHER = 1;

    public static String createKettleFilesForOracle(KettleCreateInfo createKettleFilesInfo){

        // Set parameters
        String homePath  = createKettleFilesInfo.getHomePath();
        String workspacePath = homePath + "/workspace";
        String serverPath = homePath + "/server";
        String owner = createKettleFilesInfo.getOwner();
        String tableName = createKettleFilesInfo.getTableName();
        String[] cloumns = createKettleFilesInfo.getCloumns();
        String ownerDestination = createKettleFilesInfo.getOwnerDestination();
        String incrementalCloumnName = createKettleFilesInfo.getIncrementalCloumnName();
        int incrementalCloumnCategory = createKettleFilesInfo.getIncrementalCloumnCategory();
        int cloumnNumber = cloumns.length;
        String path = workspacePath + "/" + owner + "." + tableName;

        // Create workspace path and copy template files
        String cmd ="mkdir -p " + path + " &&  rm -rf " + path + "/* && cp -r  " + serverPath + "/demo-oracle/* " + path + "/";
        executeCommand(cmd,null);

        // Create the information needed to edit the template file with parameters
        String copyPath = path + "/" + "copy_cloumn.txt";
        String selectPath = path + "/" + "copy_select.txt";
        String createPath = path + "/" + "createTable.sql";
        String[] arrCol = new String[4*cloumnNumber];
        String[] arrSelect = new String[cloumnNumber+4];
        String[] arrCreate = new String[cloumnNumber+5];
        String maxEtldate = "MAX\\("+ incrementalCloumnName +"\\)";
        String vardate ="";
        for(int j = 0;j < cloumnNumber;j++){
            arrCol[j*4+0] = "      <field>";
            arrCol[j*4+1] = "        <column_name>" + cloumns[j].toLowerCase() + "</column_name>";
            arrCol[j*4+2] = "        <stream_name>" + cloumns[j] + "</stream_name>";
            arrCol[j*4+3] = "      </field>";
            arrSelect[j+1] = "\"" + cloumns[j] + "\",";
            arrCreate[j+2] = "\"" + cloumns[j].toLowerCase() + "\" text,";
        }
        arrSelect[0] = "    <sql>SELECT ";
        arrSelect[cloumnNumber] = arrSelect[cloumnNumber].substring(0,arrSelect[cloumnNumber].length()-1);
        arrSelect[cloumnNumber+1] = " FROM "+ owner + "." + tableName;
        arrCreate[0] = "CREATE TABLE " + ownerDestination + "." + tableName+"(";
        arrCreate[1] = "\"num_id\" numeric(16),";
        arrCreate[cloumnNumber+1] = arrCreate[cloumnNumber+1].substring(0,arrCreate[cloumnNumber+1].length()-1);
        arrCreate[cloumnNumber+2] = ");";
        arrCreate[cloumnNumber+3] = "CREATE SEQUENCE " + ownerDestination+"_" + tableName + "_SEQ START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;";
        arrCreate[cloumnNumber+4] = "ALTER TABLE " + ownerDestination + "." + tableName
                +" ALTER COLUMN NUM_ID SET DEFAULT nextval('" + ownerDestination + "_" + tableName + "_SEQ');";
        if(StringUtils.isNotBlank(incrementalCloumnName)){
            if(CLOUMN_CATEGORY_DATESTRING == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " NVL\\(MAX\\(VARDATE\\),\\'19000101000000\\'\\) ";
            }else if (CLOUMN_CATEGORY_DATE == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE "+ incrementalCloumnName +" > TO_DATE('${STARTDATE}','YYYYMMDDHH24MISS') ";
                arrSelect[cloumnNumber+3] = "AND "+ incrementalCloumnName +"  &lt;= TO_DATE('${ENDDATE}','YYYYMMDDHH24MISS')</sql>";
                vardate = " NVL\\(to_CHAR\\(MAX\\(VARDATE\\),\\'YYYYMMDDHH24MISS\\'\\),\\'19000101000000\\'\\)";
            }else if (CLOUMN_CATEGORY_OTHER == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " NVL\\(MAX\\(VARDATE\\),\\'0\\'\\) ";
                // Create the startup script file
                cmd = " echo 'STARTDATE' > " + path + "/demo_start.txt " +
                        " && echo ' ' >> " + path + "/demo_start.txt ";
                executeCommand(cmd,null);
            }
        }else{
            arrSelect[cloumnNumber+2] = " ";
            arrSelect[cloumnNumber+3] = " </sql>";
            vardate = "MAX\\(VARDATE\\)";
            maxEtldate = " \\'19000101000000\\'" ;
        }

        // Edit the template file
        // Add query information
        try {
            writeFile(arrSelect,selectPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Add field matching information
        try {
            writeFile(arrCol,copyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Merge files
        cmd = " " + path + "/create.sh"
                + " " + path
                + " " + owner
                + " " + owner + String.valueOf(createKettleFilesInfo.getDatabaseId()) + "_JNDI"
                + " " + tableName
                + " " + ownerDestination
                + " " + maxEtldate
                + " " + vardate;
        executeCommand(cmd,null);
        // Create the startup script file
        cmd = " sed -i '/" + owner + "." + tableName +"/d' "+serverPath + "/start-all.sh " +
                " && echo 'output=`" + path + "/start.sh $PDI_HOME `' >> " + serverPath + "/start-all.sh ";
        executeCommand(cmd,null);
        cmd = " sed -i '/" + owner + "." + tableName + "/d' " + serverPath + "/start_all_custom.sh " +
                " && echo 'output=`"+ path + "/start_custom.sh $PDI_HOME $STARTDATE $ENDDATE `' >> " + serverPath + "/start_all_custom.sh " +
                " && echo $output >> all.log  ";
        executeCommand(cmd,null);
        // Delete temporary files
        cmd = "cd "+ path+" &&  rm -rf create.sh copy_cloumn.txt copy_select.txt";
        executeCommand(cmd,null);
        // Create the postgres create table SQL file
        try {
            writeFile(arrCreate,createPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create table on  postgres
        String[] postgresSql = {" " , " " , " "};
        for(int i = 0 ; i < arrCreate.length - 2 ; i ++ ){
            postgresSql[0] = postgresSql[0] + arrCreate[i];
        }
        postgresSql[1] = arrCreate[arrCreate.length-2];
        postgresSql[2] = arrCreate[arrCreate.length-1];
        createPostgresTable(createKettleFilesInfo.getDbConnInfo(),postgresSql);

        return path;
    }

    /**
     *
     * @Description: create kettle files with database is H2
     * @param createKettleFilesInfo the parameters required by the method
     * @return  kettle files infomation
     */
    public static String createKettleFilesForH2(KettleCreateInfo createKettleFilesInfo){

        // Set parameters

        String homePath  = createKettleFilesInfo.getHomePath();
        String workspacePath = homePath + "/workspace";
        String serverPath = homePath + "/server";
        String owner = createKettleFilesInfo.getOwner();
        String tableName = createKettleFilesInfo.getTableName();
        String[] cloumns = createKettleFilesInfo.getCloumns();
        String ownerDestination = createKettleFilesInfo.getOwnerDestination();
        String incrementalCloumnName = createKettleFilesInfo.getIncrementalCloumnName();
        int incrementalCloumnCategory = createKettleFilesInfo.getIncrementalCloumnCategory();
        int cloumnNumber = cloumns.length;
        String path = workspacePath + "/" + owner + "." + tableName;

        // Create workspace path and copy template files
        String cmd ="mkdir -p " + path + " &&  rm -rf " + path + "/*  && cp -r  " + serverPath + "/demo-h2/* " + path + "/";
        executeCommand(cmd,null);

        // Create the information needed to edit the template file with parameters
        String copyPath = path + "/" + "copy_cloumn.txt";
        String selectPath = path + "/" + "copy_select.txt";
        String createPath = path + "/" + "createTable.sql";
        String[] arrCol = new String[4*cloumnNumber];
        String[] arrSelect = new String[cloumnNumber+4];
        String[] arrCreate = new String[cloumnNumber+5];
        String maxEtldate = "MAX\\("+ incrementalCloumnName +"\\)";
        String vardate ="";
        for(int j = 0;j < cloumnNumber;j++){
            arrCol[j*4+0] = "      <field>";
            arrCol[j*4+1] = "        <column_name>" + cloumns[j].toLowerCase() + "</column_name>";
            arrCol[j*4+2] = "        <stream_name>" + cloumns[j] + "</stream_name>";
            arrCol[j*4+3] = "      </field>";
            arrSelect[j+1] = "\"" + cloumns[j] + "\",";
            arrCreate[j+2] = "\"" + cloumns[j].toLowerCase() + "\" text,";
        }
        arrSelect[0] = "    <sql>SELECT ";
        arrSelect[cloumnNumber] = arrSelect[cloumnNumber].substring(0,arrSelect[cloumnNumber].length()-1);
        arrSelect[cloumnNumber+1] = " FROM "+ owner + "." + tableName;
        arrCreate[0] = "CREATE TABLE " + ownerDestination + "." + tableName+"(";
        arrCreate[1] = "\"num_id\" numeric(16),";
        arrCreate[cloumnNumber+1] = arrCreate[cloumnNumber+1].substring(0,arrCreate[cloumnNumber+1].length()-1);
        arrCreate[cloumnNumber+2] = ");";
        arrCreate[cloumnNumber+3] = "CREATE SEQUENCE " + ownerDestination+"_" + tableName + "_SEQ START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;";
        arrCreate[cloumnNumber+4] = "ALTER TABLE " + ownerDestination + "." + tableName
                +" ALTER COLUMN NUM_ID SET DEFAULT nextval('" + ownerDestination + "_" + tableName + "_SEQ');";
        if(StringUtils.isNotBlank(incrementalCloumnName)){
            if(CLOUMN_CATEGORY_DATESTRING == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " NVL\\(MAX\\(VARDATE\\),\\'19000101000000\\'\\) ";
            }else if (CLOUMN_CATEGORY_DATE == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE TO_CHAR(" + incrementalCloumnName + ",'YYYYMMDDHH24MISS') > '${STARTDATE}' ";
                arrSelect[cloumnNumber+3] = "AND TO_CHAR(" + incrementalCloumnName + ",'YYYYMMDDHH24MISS')  &lt;= '${ENDDATE}'</sql>";
                vardate = " NVL\\(to_CHAR\\(MAX\\(VARDATE\\),\\'YYYYMMDDHH24MISS\\'\\),\\'19000101000000\\'\\)";
            }else if (CLOUMN_CATEGORY_OTHER == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " NVL\\(MAX\\(VARDATE\\),\\'0\\'\\) ";
                // Create the startup script file
                cmd = " echo 'STARTDATE' > " + path + "/demo_start.txt " +
                        " && echo ' ' >> " + path + "/demo_start.txt ";
                executeCommand(cmd,null);
            }
        }else{
            arrSelect[cloumnNumber+2] = " ";
            arrSelect[cloumnNumber+3] = " </sql>";
            vardate = "MAX\\(VARDATE\\)";
            maxEtldate = " \\'19000101000000\\'" ;
        }

        // Edit the template file
        // Add query information
        try {
            writeFile(arrSelect,selectPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Add field matching information
        try {
            writeFile(arrCol,copyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Merge files
        cmd = " " + path + "/create.sh"
                + " " + path
                + " " + owner
                + " " + owner +  String.valueOf(createKettleFilesInfo.getDatabaseId()) + "_JNDI"
                + " " + tableName
                + " " + ownerDestination
                + " " + maxEtldate
                + " " + vardate;
        executeCommand(cmd,null);
        // Create the startup script file
        cmd = " sed -i '/" + owner + "." + tableName +"/d' "+serverPath + "/start-all.sh " +
                " && echo 'output=`" + path + "/start.sh $PDI_HOME `' >> " + serverPath + "/start-all.sh ";
        executeCommand(cmd,null);
        cmd = " sed -i '/" + owner + "." + tableName + "/d' " + serverPath + "/start_all_custom.sh " +
                " && echo 'output=`"+ path + "/start_custom.sh $PDI_HOME $STARTDATE $ENDDATE `' >> " + serverPath + "/start_all_custom.sh " +
                " && echo $output >> all.log  ";
        executeCommand(cmd,null);
        // Delete temporary files
        cmd = "cd "+ path+" &&  rm -rf create.sh copy_cloumn.txt copy_select.txt";
        executeCommand(cmd,null);
        // Create the postgres create table SQL file
        try {
            writeFile(arrCreate,createPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create table on postgres
        String[] postgresSql = {" " , " " , " "};
        for(int i = 0 ; i < arrCreate.length - 2 ; i ++ ){
            postgresSql[0] = postgresSql[0] + arrCreate[i];
        }
        postgresSql[1] = arrCreate[arrCreate.length-2];
        postgresSql[2] = arrCreate[arrCreate.length-1];
        createPostgresTable(createKettleFilesInfo.getDbConnInfo(),postgresSql);

        return path;
    }





    /**
     * @Description: create kettle files with database is mysql
     * @param createKettleFilesInfo the parameters required by the method
     * @return  kettle files infomation
     */

    public static String createKettleFilesForMysql(KettleCreateInfo createKettleFilesInfo){

        // Set parameters
        String homePath  = createKettleFilesInfo.getHomePath();
        String workspacePath = homePath + "/workspace";
        String serverPath = homePath + "/server";
        String owner = createKettleFilesInfo.getOwner();
        String tableName = createKettleFilesInfo.getTableName();
        String[] cloumns = createKettleFilesInfo.getCloumns();
        String ownerDestination = createKettleFilesInfo.getOwnerDestination();
        String incrementalCloumnName = createKettleFilesInfo.getIncrementalCloumnName();
        int incrementalCloumnCategory = createKettleFilesInfo.getIncrementalCloumnCategory();
        int cloumnNumber = cloumns.length;
        String path = workspacePath + "/" + owner + "." + tableName;

        // Create workspace path and copy template files
        String cmd ="mkdir -p " + path + " &&  rm -rf " + path + "/* && cp -r  " + serverPath + "/demo-mysql/* " + path + "/";
        executeCommand(cmd,null);

        // Create the information needed to edit the template file with parameters
        String copyPath = path + "/" + "copy_cloumn.txt";
        String selectPath = path + "/" + "copy_select.txt";
        String createPath = path + "/" + "createTable.sql";
        String[] arrCol = new String[4*cloumnNumber];
        String[] arrSelect = new String[cloumnNumber+4];
        String[] arrCreate = new String[cloumnNumber+5];
        String maxEtldate = "MAX\\("+ incrementalCloumnName +"\\)";
        String vardate ="";
        for(int j = 0;j < cloumnNumber;j++){
            arrCol[j*4+0] = "      <field>";
            arrCol[j*4+1] = "        <column_name>" + cloumns[j].toLowerCase() + "</column_name>";
            arrCol[j*4+2] = "        <stream_name>" + cloumns[j] + "</stream_name>";
            arrCol[j*4+3] = "      </field>";
            arrSelect[j+1] = "`" + cloumns[j] + "`,";
            arrCreate[j+2] = "\"" + cloumns[j].toLowerCase() + "\" text,";
        }
        arrSelect[0] = "    <sql>SELECT ";
        arrSelect[cloumnNumber] = arrSelect[cloumnNumber].substring(0,arrSelect[cloumnNumber].length()-1);
        arrSelect[cloumnNumber+1] = " FROM "+ owner + "." + tableName;
        arrCreate[0] = "CREATE TABLE " + ownerDestination + "." + tableName+"(";
        arrCreate[1] = "\"num_id\" numeric(16),";
        arrCreate[cloumnNumber+1] = arrCreate[cloumnNumber+1].substring(0,arrCreate[cloumnNumber+1].length()-1);
        arrCreate[cloumnNumber+2] = ");";
        arrCreate[cloumnNumber+3] = "CREATE SEQUENCE " + ownerDestination+"_" + tableName + "_SEQ START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;";
        arrCreate[cloumnNumber+4] = "ALTER TABLE " + ownerDestination + "." + tableName
                +" ALTER COLUMN NUM_ID SET DEFAULT nextval('" + ownerDestination + "_" + tableName + "_SEQ');";
        if(StringUtils.isNotBlank(incrementalCloumnName)){
            if(CLOUMN_CATEGORY_DATESTRING == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " IFNULL\\(MAX\\(VARDATE\\),\\'19000101000000\\'\\) ";
            }else if (CLOUMN_CATEGORY_DATE == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE "+ incrementalCloumnName +" > STR_TO_DATE('${STARTDATE}','%Y%m%d%H%i%s') ";
                arrSelect[cloumnNumber+3] = "AND "+ incrementalCloumnName +"  &lt;= STR_TO_DATE('${ENDDATE}','%Y%m%d%H%i%s')</sql>";
                vardate = " IFNULL\\(DATE_FORMAT\\(MAX\\(VARDATE\\),\\'%Y%m%d%H%i%s\\'\\),\\'19000101000000\\'\\)";
            }else if (CLOUMN_CATEGORY_OTHER == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " IFNULL\\(MAX\\(VARDATE\\),\\'0\\'\\) ";
                // Create the startup script file
                cmd = " echo 'STARTDATE' > " + path + "/demo_start.txt " +
                        " && echo ' ' >> " + path + "/demo_start.txt ";
                executeCommand(cmd,null);
            }
        }else{
            arrSelect[cloumnNumber+2] = " ";
            arrSelect[cloumnNumber+3] = " </sql>";
            vardate = "MAX\\(VARDATE\\)";
            maxEtldate = " \\'19000101000000\\'" ;
        }

        // Edit the template file
        // Add query information
        try {
            writeFile(arrSelect,selectPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Add field matching information
        try {
            writeFile(arrCol,copyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Merge files
        cmd = " " + path + "/create.sh"
                + " " + path
                + " " + owner
                + " " + owner +  String.valueOf(createKettleFilesInfo.getDatabaseId()) + "_JNDI"
                + " " + tableName
                + " " + ownerDestination
                + " " + maxEtldate
                + " " + vardate;
        executeCommand(cmd,null);
        // Create the startup script file
        cmd = " sed -i '/" + owner + "." + tableName +"/d' "+serverPath + "/start-all.sh " +
                " && echo 'output=`" + path + "/start.sh $PDI_HOME `' >> " + serverPath + "/start-all.sh ";
        executeCommand(cmd,null);
        cmd = " sed -i '/" + owner + "." + tableName + "/d' " + serverPath + "/start_all_custom.sh " +
                " && echo 'output=`"+ path + "/start_custom.sh $PDI_HOME $STARTDATE $ENDDATE `' >> " + serverPath + "/start_all_custom.sh " +
                " && echo $output >> all.log  ";
        executeCommand(cmd,null);
        // Delete temporary files
        cmd = "cd "+ path+" &&  rm -rf create.sh copy_cloumn.txt copy_select.txt ";

        executeCommand(cmd,null);
        // Create the postgres create table SQL file
        try {
            writeFile(arrCreate,createPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create table on  postgres
        String[] postgresSql = {" " , " " , " "};
        for(int i = 0 ; i < arrCreate.length - 2 ; i ++ ){
            postgresSql[0] = postgresSql[0] + arrCreate[i];
        }
        postgresSql[1] = arrCreate[arrCreate.length-2];
        postgresSql[2] = arrCreate[arrCreate.length-1];
        createPostgresTable(createKettleFilesInfo.getDbConnInfo(),postgresSql);

        return path;
    }

    /**
     * @Description: create kettle files with database is postgres
     * @param createKettleFilesInfo the parameters required by the method
     * @return  kettle files infomation
     */

    public static String createKettleFilesForPostgres(KettleCreateInfo createKettleFilesInfo){

        // Set parameters
        String homePath  = createKettleFilesInfo.getHomePath();
        String workspacePath = homePath + "/workspace";
        String serverPath = homePath + "/server";
        String owner = createKettleFilesInfo.getOwner();
        String tableName = createKettleFilesInfo.getTableName();
        String[] cloumns = createKettleFilesInfo.getCloumns();
        String ownerDestination = createKettleFilesInfo.getOwnerDestination();
        String incrementalCloumnName = createKettleFilesInfo.getIncrementalCloumnName();
        int incrementalCloumnCategory = createKettleFilesInfo.getIncrementalCloumnCategory();
        int cloumnNumber = cloumns.length;
        String path = workspacePath + "/" + owner + "." + tableName;

        // Create workspace path and copy template files
        String cmd ="mkdir -p " + path + " &&  rm -rf " + path + "/* && cp -r  " + serverPath + "/demo-postgres/* " + path + "/";
        executeCommand(cmd,null);

        // Create the information needed to edit the template file with parameters
        String copyPath = path + "/" + "copy_cloumn.txt";
        String selectPath = path + "/" + "copy_select.txt";
        String createPath = path + "/" + "createTable.sql";
        String[] arrCol = new String[4*cloumnNumber];
        String[] arrSelect = new String[cloumnNumber+4];
        String[] arrCreate = new String[cloumnNumber+5];
        String maxEtldate = "MAX\\("+ incrementalCloumnName +"\\)";
        String vardate ="";
        for(int j = 0;j < cloumnNumber;j++){
            arrCol[j*4+0] = "      <field>";
            arrCol[j*4+1] = "        <column_name>" + cloumns[j].toLowerCase() + "</column_name>";
            arrCol[j*4+2] = "        <stream_name>" + cloumns[j] + "</stream_name>";
            arrCol[j*4+3] = "      </field>";
            arrSelect[j+1] = "\"" + cloumns[j] + "\",";
            arrCreate[j+2] = "\"" + cloumns[j].toLowerCase() + "\" text,";
        }
        arrSelect[0] = "    <sql>SELECT ";
        arrSelect[cloumnNumber] = arrSelect[cloumnNumber].substring(0,arrSelect[cloumnNumber].length()-1);
        arrSelect[cloumnNumber+1] = " FROM "+ owner + "." + tableName;
        arrCreate[0] = "CREATE TABLE " + ownerDestination + "." + tableName+"(";
        arrCreate[1] = "\"num_id\" numeric(16),";
        arrCreate[cloumnNumber+1] = arrCreate[cloumnNumber+1].substring(0,arrCreate[cloumnNumber+1].length()-1);
        arrCreate[cloumnNumber+2] = ");";
        arrCreate[cloumnNumber+3] = "CREATE SEQUENCE " + ownerDestination+"_" + tableName + "_SEQ START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;";
        arrCreate[cloumnNumber+4] = "ALTER TABLE " + ownerDestination + "." + tableName
                +" ALTER COLUMN NUM_ID SET DEFAULT nextval('" + ownerDestination + "_" + tableName + "_SEQ');";
        if(StringUtils.isNotBlank(incrementalCloumnName)){
            if(CLOUMN_CATEGORY_DATESTRING == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " COALESCE\\(MAX\\(VARDATE\\),\\'19000101000000\\'\\) ";
            }else if (CLOUMN_CATEGORY_DATE == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE "+ incrementalCloumnName +" > TO_TIMESTAMP('${STARTDATE}','YYYYMMDDHH24MISS') ";
                arrSelect[cloumnNumber+3] = "AND "+ incrementalCloumnName +"  &lt;= TO_TIMESTAMP('${ENDDATE}','YYYYMMDDHH24MISS')</sql>";
                vardate = " COALESCE\\(to_CHAR\\(MAX\\(VARDATE\\),\\'YYYYMMDDHH24MISS\\'\\),\\'19000101000000\\'\\)";
            }else if (CLOUMN_CATEGORY_OTHER == incrementalCloumnCategory){
                arrSelect[cloumnNumber+2] = " WHERE " + incrementalCloumnName + " >'${STARTDATE}'";
                arrSelect[cloumnNumber+3] = "AND " + incrementalCloumnName + " &lt;= '${ENDDATE}'</sql>";
                vardate = " COALESCE\\(MAX\\(VARDATE\\),\\'0\\'\\) ";
                // Create the startup script file
                cmd = " echo 'STARTDATE' > " + path + "/demo_start.txt " +
                        " && echo ' ' >> " + path + "/demo_start.txt ";
                executeCommand(cmd,null);
            }
        }else{
            arrSelect[cloumnNumber+2] = " ";
            arrSelect[cloumnNumber+3] = " </sql>";
            vardate = "MAX\\(VARDATE\\)";
            maxEtldate = " \\'19000101000000\\'" ;
        }

        // Edit the template file
        // Add query information
        try {
            writeFile(arrSelect,selectPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Add field matching information
        try {
            writeFile(arrCol,copyPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Merge files
        cmd = " " + path + "/create.sh"
                + " " + path
                + " " + owner
                + " " + owner +  String.valueOf(createKettleFilesInfo.getDatabaseId()) + "_JNDI"
                + " " + tableName
                + " " + ownerDestination
                + " " + maxEtldate
                + " " + vardate;
        executeCommand(cmd,null);
        // Create the startup script file
        cmd = " sed -i '/" + owner + "." + tableName +"/d' "+serverPath + "/start-all.sh " +
                " && echo 'output=`" + path + "/start.sh $PDI_HOME `' >> " + serverPath + "/start-all.sh ";
        executeCommand(cmd,null);
        cmd = " sed -i '/" + owner + "." + tableName + "/d' " + serverPath + "/start_all_custom.sh " +
                " && echo 'output=`"+ path + "/start_custom.sh $PDI_HOME $STARTDATE $ENDDATE `' >> " + serverPath + "/start_all_custom.sh " +
                " && echo $output >> all.log  ";
        executeCommand(cmd,null);
        // Delete temporary files
        cmd = "cd "+ path+" &&  rm -rf create.sh copy_cloumn.txt copy_select.txt";
        executeCommand(cmd,null);
        // Create the postgres create table SQL file
        try {
            writeFile(arrCreate,createPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create table on  postgres
        String[] postgresSql = {" " , " " , " "};
        for(int i = 0 ; i < arrCreate.length - 2 ; i ++ ){
            postgresSql[0] = postgresSql[0] + arrCreate[i];
        }
        postgresSql[1] = arrCreate[arrCreate.length-2];
        postgresSql[2] = arrCreate[arrCreate.length-1];
        createPostgresTable(createKettleFilesInfo.getDbConnInfo(),postgresSql);
        return path;
    }

    /**
     * @Description: start kettle job
     * @param homePath  home path
     * @param path script files storage path
     * @param jobId
     */
    public  static void startKettleJob(String homePath,String path,int jobId,String url ){
        System.out.println("============startJob============:" +path);
        String cmd = homePath + "/server/start.sh " + path + "/start.sh " + homePath + "/data-integration " + jobId + " " + url;
        executeCommand(cmd,null);
    }


    /**
     * @Description: execute  command on LINUX OS
     * @param cmd  commands to be executed
     * @param dir  file
     * @return command execution result
     */
    public static void executeCommand(String cmd, File dir) {
        ArrayList<String> arr = new ArrayList<String>();
        Process process = null;
        BufferedReader bufrIn = null;
        try {
            String[] command = {"/bin/sh", "-c", cmd };
            process = Runtime.getRuntime().exec(command, null, dir);
            process.waitFor();
            bufrIn = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
            String line = null;
            while ((line = bufrIn.readLine()) != null) {
                arr.add(line);
            }
        } catch (Exception e) {
            System.out.println("Execution command failed !");
        } finally {
            if (bufrIn != null) {
                try {
                    bufrIn.close();
                } catch (Exception e) {
                }
            }
            if (process != null) {
                process.destroy();
            }
        }
        System.out.println("The command is :| " + cmd);
    }

    /**
     * @Description: write new file on LINUX OS
     * @param fileLines  file lines
     * @param path  file storage  path
     * @throws IOException
     */
    public static void  writeFile(String[] fileLines,String path) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(path));
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, "UTF-8");
        BufferedWriter  bufferedWriter = new BufferedWriter(outputStreamWriter);
        for(String fileLine:fileLines){
            bufferedWriter.write(fileLine + "\t\n");
            System.out.println("Write file succeed ,the line is :| " + fileLine);
        }
        bufferedWriter.close();
        outputStreamWriter.close();
        fileOutputStream.close();
    }

    /**
     * @Description: Create table on postgres
     * @param dbConnInfo  postgres connection info
     * @param postgresSql SQL
     */
    private static void createPostgresTable(DbConnInfo dbConnInfo, String[] postgresSql) {
        Connection conn = null;
        PreparedStatement pst = null;
        try {
            Class.forName(dbConnInfo.getDbDriver());
            conn = DriverManager.getConnection(dbConnInfo.getUrl(), dbConnInfo.getUserName(), dbConnInfo.getPassWord());
            for(int i = 0 ; i < postgresSql.length ; i ++ ){
                pst = conn.prepareStatement(postgresSql[i]);
                pst.execute();
                pst.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
