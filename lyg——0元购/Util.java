package com.wochacha.da.transform.lyg;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Util
{
    /**
     * 正则匹配
     * @param str
     * @param patStr
     * @return
     */
    public static boolean regMatch(String str, String patStr)
    {
        Pattern pattern = Pattern.compile(patStr);
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }
    
    /**
     * 正则分割
     * @param str
     * @param patStr
     * @return
     */
    public static String[] regSplit(String str, String patStr)
    {
        Pattern pattern = Pattern.compile(patStr);
        String[] splitStrings = pattern.split(str);
        return splitStrings;
    }
    
    /**
     * 正则替换
     * @param str
     * @param patString
     * @param replacement
     * @return
     */
    public static String regReplace(String str, String patString, String replacement)
    {
        Pattern pattern = Pattern.compile(patString);
        Matcher matcher = pattern.matcher(str);
        return matcher.replaceAll(replacement);
    }
    
    /**
     * 打印一维数组
     * @param objects
     */
    public static void printArray(Object[] objects)
    {
        System.out.print( "{");
        for (int i = 0, len=objects.length; i < len; i++)
        {
            System.out.print("[" + i + "]=>");
            System.out.print(objects[i]);
            if( i<len-1 )
            {
                System.out.print( "," );
            }
        }
        System.out.print( "}" );
    }
    
    /**
     * 打印二维数组
     * @param objects
     */
    public static void printArray(Object[][] objects)
    {
        System.out.print( "[");
        for (int i = 0, len=objects.length; i < len; i++)
        {
            printArray(objects[i]);
            if( i<len-1 )
            {
                System.out.print( "," );
            }
        }
        System.out.print( "]" );
    }
    
    /**
     * 检查数据库驱动程序包是否加载(mysql)
     * @return
     */
    public static boolean checkDbDriver()
    {
        String dbDriverString = "org.gjt.mm.mysql.Driver";
        try
        {
            Class.forName(dbDriverString);
        }
        catch (ClassNotFoundException e)
        {
            return false;
        }
        return true;
    }
    
    public Connection connectDb(String host, String db, String username, String password)
    {
        String dbUrl = "jdbc:mysql://" + host + "/" + db;
        Connection connection = null;
        try
        {
            connection = DriverManager.getConnection(dbUrl, username, password);
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return connection;
    }
    
    public void insert(String sql)
    {
        
    }
    
    public void close()
    {
        
    }
    
}
