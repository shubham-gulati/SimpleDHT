package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.DatabaseErrorHandler;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class KVStorageSqliteOpenHelper extends SQLiteOpenHelper {

    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_NAME = "KVSTORAGE_DB";


    public KVStorageSqliteOpenHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    public KVStorageSqliteOpenHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version, DatabaseErrorHandler errorHandler) {
        super(context, DATABASE_NAME, factory, DATABASE_VERSION, errorHandler);
    }

    public static final String TABLE_KVSTORAGE = "KVSTORAGE";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";

    private static final String[] COLUMNS = {COLUMN_KEY,COLUMN_VALUE};

    @Override
    public void onCreate(SQLiteDatabase db) {
        //Log.e("creating table","starting");
        String CREATE_KVSTORAGE_TABLE = "CREATE TABLE "+TABLE_KVSTORAGE+" ( " +
                COLUMN_KEY+" TEXT PRIMARY KEY, "+
                COLUMN_VALUE+" TEXT )";

        // create books table
        db.execSQL(CREATE_KVSTORAGE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        try {
            //Log.e("in on upgrade", "starting");
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_KVSTORAGE);
            this.onCreate(db);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void add(ContentValues contentValues) {
        //Log.e("inside add", "inside");
        try {
            SQLiteDatabase db = this.getWritableDatabase();
            //check if key already exits then delete from Sqlite DB
            db.insertWithOnConflict(TABLE_KVSTORAGE, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
            db.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            Log.e("exception in add", "inside");
            e.printStackTrace();
        }
    }

}
