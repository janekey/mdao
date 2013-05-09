package com.janekey.mdao.dao;

import com.janekey.mdao.annotation.AnnotaionParseException;
import com.janekey.mdao.annotation.Column;
import com.janekey.mdao.annotation.Table;
import com.janekey.mdao.connection.DBConnection;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseDao {
	
	private static Logger LOGGER = Logger.getLogger(BaseDao.class);
	
	public static final String DB = "db";
	
	/**
	 * 适用于执行更新一条语句
	 */
	protected int executeUpdate(String sql, Object ... object) {
		DBConnection dbcon = null;
		int row = 0;
		try {
			dbcon = new DBConnection(DB);
			dbcon.prepareStatement(sql);
			dbcon.setParams(object);
			row = dbcon.executeUpdate();
		} catch (SQLException e) {
            LOGGER.error(e.getMessage());
		} finally {
			if (dbcon != null) {
				dbcon.free();
				dbcon = null;
			}
		}
		return row;
	}
	
	/**
	 * 查询并返回结果集(有参数)
	 */
	protected List<Map<String, Object>> executeQuery(String sql, Object ... object) {
		DBConnection dbcon = null;
		List<Map<String, Object>> rsList = new ArrayList<Map<String, Object>>();
		try {
			dbcon = new DBConnection(DB);
			dbcon.prepareStatement(sql);
			dbcon.setParams(object);
			ResultSet rs = dbcon.executeQuery();
			
			ResultSetMetaData rsmd = dbcon.getPrepStmt().getMetaData();
			int columnCount = rsmd.getColumnCount();

			while(rs.next()) {
                Map<String, Object> rowMap = new HashMap<String, Object>();
				for(int i = 0; i < columnCount; i++) {
                    String columnLabel = rsmd.getColumnLabel(i + 1);
                    Object obj = rs.getObject(i + 1);
                    rowMap.put(columnLabel, obj);
				}
				rsList.add(rowMap);
			}
		} catch (SQLException e) {
            LOGGER.error(e.getMessage());
		} finally {
			if (dbcon != null) {
				dbcon.free();
				dbcon = null;
			}
		}
		return rsList;
	}

    protected int selectCount(String sql, Object ... object) {
        DBConnection dbcon = null;
        int count = 0;
        try {
            dbcon = new DBConnection(DB);
            dbcon.prepareStatement(sql);
            dbcon.setParams(object);
            ResultSet rs = dbcon.executeQuery();

            if(rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        } finally {
            if (dbcon != null) {
                dbcon.free();
                dbcon = null;
            }
        }
        return count;
    }

    protected List selectList(String sql, Class cl, Object ... object) {
        DBConnection dbcon = null;
        List<Object> rsList = new ArrayList<Object>();
        try {
            dbcon = new DBConnection(DB);
            dbcon.prepareStatement(sql);
            dbcon.setParams(object);
            ResultSet rs = dbcon.executeQuery();

            ResultSetMetaData rsmd = dbcon.getPrepStmt().getMetaData();
            int columnCount = rsmd.getColumnCount();

            while(rs.next()) {
                Map<String, Object> rowMap = new HashMap<String, Object>();
                for(int i = 0; i < columnCount; i++) {
                    String columnLabel = rsmd.getColumnLabel(i + 1);
                    Object obj = rs.getObject(i + 1);
                    rowMap.put(columnLabel, obj);
                }
                Object instance = cl.newInstance();
                fillObject(instance, rowMap);
                rsList.add(instance);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {
            if (dbcon != null) {
                dbcon.free();
                dbcon = null;
            }
        }
        return rsList;
    }

	/**
	 * 查询并返回结果集(无参数)
	 */
	protected List<Object[]> executeQuery(String sql) {
		DBConnection dbcon = null;
		List<Object[]> rsList = new ArrayList<Object[]>();
		try {
			dbcon = new DBConnection(DB);
			dbcon.prepareStatement(sql);
			ResultSet rs = dbcon.executeQuery();
			
			ResultSetMetaData rsmd = dbcon.getPrepStmt().getMetaData();
			int columnCount = rsmd.getColumnCount();
			
			while(rs.next()) {
				Object[] objs = new Object[columnCount];
				for(int i=0; i<columnCount; i++) {
					objs[i] = rs.getObject(i+1);
				}
				rsList.add(objs);
			}
		} catch (SQLException e) {
            LOGGER.error(e.getMessage());
		} finally {
			if (dbcon != null) {
				dbcon.free();
				dbcon = null;
			}
		}
		return rsList;
	}
	
	/**
	 * 插入一条语句
	 * @return 插入后的ID 
	 */
	protected int executeInsert(String sql, Object ... object) {
		DBConnection dbcon = null;
		int id = 0;
		try {
			dbcon = new DBConnection(DB);
			dbcon.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			dbcon.setParams(object);
			id = dbcon.executeInsert();
		} catch (SQLException e) {
            LOGGER.error(e.getMessage());
		} finally {
			if (dbcon != null) {
				dbcon.free();
				dbcon = null;
			}
		}
		return id;
	}

    /**
     * 将object数据插入匹配的数据库表中
     * 该object对象使用注解匹配数据库表及字段
     * @return 插入后的ID
     */
    protected int executeInsert(Object object) {
        DBConnection dbcon = null;
        int id = 0;
        try {
            List<Object> params = new ArrayList<Object>();
            String sql = inserSql(params, object);
            dbcon = new DBConnection(DB);
            dbcon.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            dbcon.setParams(params.toArray());
            id = dbcon.executeInsert();
        } catch (Exception e) {
            LOGGER.error(e);
            return 0;
        } finally {
            if (dbcon != null) {
                dbcon.free();
                dbcon = null;
            }
        }
        return id;
    }

    private String inserSql(List<Object> params, Object object) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        Class<?> cl = object.getClass();
        // Get table name annnotation
        Table table = cl.getAnnotation(Table.class);
        if (table == null) {
            throw new AnnotaionParseException("Count not find table annotation in model class");
        }
        sql.append(table.name());

        StringBuilder values = new StringBuilder();
        sql.append(" (");
        values.append(" (");
        for (Field field : cl.getDeclaredFields()) {
            // Get column annotation
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                try {
                    String methodName = getFieldGetMethod(field);
                    Method method = cl.getDeclaredMethod(methodName);
                    Object obj = method.invoke(object);

                    if (obj != null) {
                        sql.append(column.column()).append(",");
                        values.append("?,");
                        params.add(obj);
                    }
                } catch (NoSuchMethodException e) {
                    LOGGER.error(e.getMessage());
                } catch (IllegalAccessException e) {
                    LOGGER.error(e.getMessage());
                } catch (IllegalArgumentException e) {
                    LOGGER.error(e.getMessage());
                } catch (InvocationTargetException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        }
        sql.deleteCharAt(sql.length() - 1).append(")");
        values.deleteCharAt(values.length() - 1).append(")");

        sql.append(" VALUES ").append(values);
        return sql.toString();
    }

    private String updateSql(List<Object> params, Object object) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ");
        Class<?> cl = object.getClass();
        // Get table name annnotation
        Table table = cl.getAnnotation(Table.class);
        if (table == null) {
            throw new AnnotaionParseException("Count not find table annotation in model class");
        }
        sql.append(table.name()).append(" SET ");

        Object id = null;
        for (Field field : cl.getDeclaredFields()) {
            // Get column annotation
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                try {
                    String methodName = getFieldGetMethod(field);
                    Method method = cl.getDeclaredMethod(methodName);
                    Object obj = method.invoke(object);

                    if (obj != null) {
                        if (column.column().equals("id")) {
                            id = obj;
                        } else {
                            sql.append(column.column()).
                                    append(" = ?,");
                            params.add(obj);
                        }
                    }
                } catch (NoSuchMethodException e) {
                    LOGGER.error(e.getMessage());
                } catch (IllegalAccessException e) {
                    LOGGER.error(e.getMessage());
                } catch (IllegalArgumentException e) {
                    LOGGER.error(e.getMessage());
                } catch (InvocationTargetException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(" WHERE id = ? ");
        params.add(id);
        return sql.toString();
    }

    /**
     * 将object数据匹配到数据库中的数据更新
     * object对象必须有id字段
     * @return 更新的行数
     */
    protected int updateObject(Object object) {
        int update = 0;
        try {
            List<Object> params = new ArrayList<Object>();
            String sql = updateSql(params, object);
            update = executeUpdate(sql, params.toArray());
        } catch (Exception e) {
            LOGGER.error(e);
            return 0;
        }
        return update;
    }

    private void fillObject(Object object, Map<String, Object> rowMap)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> cl = object.getClass();
        for (Field field : cl.getDeclaredFields()) {
            // Get column annotation
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                Object obj = rowMap.get(column.column());
                if (obj != null) {
                    String methodName = getFieldSetMethod(field);
                    Method method = cl.getDeclaredMethod(methodName, field.getType());
                    method.invoke(object, obj);
                }
            }
        }

    }

    /**
     * 获取属性的Get方法名
     * If field name is 'name', and return 'getName'.
     */
    private String getFieldGetMethod(Field field) {
        StringBuilder methodName = new StringBuilder();
        String fieldName = field.getName();
        String first = String.valueOf(fieldName.charAt(0)).toUpperCase();
        methodName.append("get").append(first);
        if (fieldName.length() > 1) {
            methodName.append(fieldName.substring(1));
        }
        return methodName.toString();
    }

    /**
     * 获取属性的Set方法名
     * If field name is 'name', and return 'setName'.
     */
    private String getFieldSetMethod(Field field) {
        StringBuilder methodName = new StringBuilder();
        String fieldName = field.getName();
        String first = String.valueOf(fieldName.charAt(0)).toUpperCase();
        methodName.append("set").append(first);
        if (fieldName.length() > 1) {
            methodName.append(fieldName.substring(1));
        }
        return methodName.toString();
    }

}
