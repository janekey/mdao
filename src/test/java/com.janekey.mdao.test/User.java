package com.janekey.mdao.test;

import com.janekey.mdao.annotation.Column;
import com.janekey.mdao.annotation.Table;

import java.util.Date;

/**
 * User: Janekey(janekey.com)
 * Date: 13-4-21
 * Time: 下午8:17
 */
@Table(name = "tb_user")
public class User {

    @Column(column = "uid")
    private Integer uid;

    @Column(column = "name")
    private String name;

    @Column(column = "pwd")
    private String pwd;

    @Column(column = "create_time")
    private Date createTime;

    public Integer getUid() {
        return uid;
    }

    public void setUid(Integer uid) {
        this.uid = uid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
}
