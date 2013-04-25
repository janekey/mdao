package com.janekey.mdao.test;

import com.janekey.mdao.dao.BaseDao;
import org.junit.Test;

import java.util.List;

/**
 * User: qi.zheng
 * Date: 13-4-24
 * Time: 下午6:22
 */
public class UserDao extends BaseDao {

//    @Test
    public void userInsertTest() {
        User user = new User();
        user.setName("janekey");
        user.setPwd("123456");
        int id = this.executeInsert(user);
    }

//    @Test
    public void userSelectTest() {
        List<User> list = selectList("select * from tb_user", User.class);
    }

}
