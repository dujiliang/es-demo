package com.czxy.repository;

import com.czxy.entity.Student;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * @Author: djl
 * @Date: 2019/3/20 18:01
 * @Version 1.0
 */
public interface StudentRepository extends ElasticsearchRepository<Student,Long> {

    /**
     * 通过名字查询学生
     * @param name
     * @return
     */
    List<Student> findByName(String name);
}
