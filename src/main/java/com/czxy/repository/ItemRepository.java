package com.czxy.repository;

import com.czxy.entity.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * @author: https://blog.csdn.net/chen_2890
 * @date: 2018/9/28 23:44
 */
public interface ItemRepository extends ElasticsearchRepository<Item,Long> {

    //public Item findByTitle(String title);

    public List<Item> findByTitle(String title);

    public List<Item> findByPriceBetween(Double minPrice,Double maxPrice);
}
