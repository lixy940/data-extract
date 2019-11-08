package com.lixy.dataextract.vo.kettle;

import com.lixy.dataextract.vo.DbConnInfo;

public class KettleCreateInfo {

    /**
     *  home path
     */
    private String homePath;
    /**
     * all the columns of the table
     */
    private String[] cloumns;
    /**
     * table source  schema name
     */
    private String owner;
    /**
     * tableName table name
     */
    private String tableName;
    /**
     * table destination  schema name
     */
    private String ownerDestination;
    /**
     * incremental column names
     */
    private String incrementalCloumnName;
    /**
     * incremental column category
     */
    private int incrementalCloumnCategory;

    /**
     * the infomation for connect to postgres
     */
    private DbConnInfo dbConnInfo;

    /**
     * database ID
     */
    private int databaseId;

    public int getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(int databaseId) {
        this.databaseId = databaseId;
    }

    public DbConnInfo getDbConnInfo() {
        return dbConnInfo;
    }

    public void setDbConnInfo(DbConnInfo dbConnInfo) {
        this.dbConnInfo = dbConnInfo;
    }

    public String getHomePath() {
        return homePath;
    }

    public void setHomePath(String homePath) {
        this.homePath = homePath;
    }

    public String[] getCloumns() {
        return cloumns;
    }

    public void setCloumns(String[] cloumns) {
        this.cloumns = cloumns;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOwnerDestination() {
        return ownerDestination;
    }

    public void setOwnerDestination(String ownerDestination) {
        this.ownerDestination = ownerDestination;
    }

    public String getIncrementalCloumnName() {
        return incrementalCloumnName;
    }

    public void setIncrementalCloumnName(String incrementalCloumnName) {
        this.incrementalCloumnName = incrementalCloumnName;
    }

    public int getIncrementalCloumnCategory() {
        return incrementalCloumnCategory;
    }

    public void setIncrementalCloumnCategory(int incrementalCloumnCategory) {
        this.incrementalCloumnCategory = incrementalCloumnCategory;
    }
}
