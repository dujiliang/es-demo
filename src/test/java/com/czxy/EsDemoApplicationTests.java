package com.czxy;


import com.czxy.entity.Item;
import com.czxy.entity.Student;
import com.czxy.repository.ItemRepository;
import com.czxy.repository.StudentRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EsDemoApplication.class)
public class EsDemoApplicationTests {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    private ItemRepository itemRepository;

    @Autowired
    private StudentRepository studentRepository;

    /**
      * @Description:创建索引，会根据Item类的@Document注解信息来创建
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:51
       */
    @Test
    public void testCreateIndex() {
        elasticsearchTemplate.createIndex(Item.class);
    }



    /**
      * @Description:配置映射，会根据Item类中的id、Field等字段来自动完成映射
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:50
       */
    @Test
    public void testPutMapping() {
        elasticsearchTemplate.putMapping(Item.class);
    }

    /**
      * @Description:删除索引
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:50
       */
    @Test
    public void testDeleteIndex() {
        elasticsearchTemplate.deleteIndex(Item.class);
    }

    /**
      * @Description:数据保存
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:50
       */
    @Test
    public void testSave(){
        Item item = new Item(1L, "小米手机7", " 手机",
                "小米", 3499.00, "http://image.baidu.com/13123.jpg");

        itemRepository.save(item);
    }

    /**
      * @Description:批量数据保存
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:49
       */
    @Test
    public void testSaveAll(){
        List<Item> list = new ArrayList<>();
        list.add(new Item(2L, "坚果手机R1", " 手机", "锤子", 3699.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", " 手机", "华为", 4499.00, "http://image.baidu.com/13123.jpg"));
        // 接收对象集合，实现批量新增
        itemRepository.saveAll(list);
    }
    /**
      * @Description:查询所有
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:49
       */
    @Test
    public void testFindAll(){
        Iterable<Item> all = itemRepository.findAll();

        for (Item item : all) {
            System.out.println(item);
        }
    }

    /**
      * @Description:查询所有含排序
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:48
       */
    @Test
    public void testSort(){
        Iterable<Item> all = itemRepository.findAll(Sort.by("price").descending());

        for (Item item : all) {
            System.out.println(item);
        }
    }


    /**
      * @Description:自定义方法查询，方法名要遵循命名规则，方法名要符合一定的约定
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:47
       */
    @Test
    public void testFindByTitle(){
        List<Item> item = itemRepository.findByTitle("米7");

        for (Item item1 : item) {

            System.out.println(item1);
        }
    }

    /**
      * @Description:区间查询
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:47
       */
    @Test
    public void testFindByPriceBetween(){
        List<Item> item = itemRepository.findByPriceBetween(3000.0,4000.0);

        for (Item item1 : item) {

            System.out.println(item1);
        }
    }

    /**
      * @Description:批量数据保存
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:46
       */
    @Test
    public void indexList() {
        List<Item> list = new ArrayList<>();
        Item item = new Item(1L, "小米手机7", "手机",
                "小米", 3499.00, "http://image.baidu.com/13123.jpg");

        list.add(item);
        list.add(new Item(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", "手机", "华为", 4499.00, "http://image.baidu.com/13123.jpg"));

        list.add(new Item(4L, "小米手机7facebook", "手机", "小米", 3299.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(5L, "坚果facebook手机R1", "手机", "锤子", 3699.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(6L, "华为META10facebook", "手机", "华为", 4499.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(7L, "facebook小米Mix2S", "手机", "小米", 4299.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(8L, "荣耀V10facebook", "手机", "华为", 2799.00, "http://image.baidu.com/13123.jpg"));

        list.add(new Item(9L, "小米手机7facebook", "手机", "小米", 3299.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(10L, "坚果facebook手机R1", "手机", "锤子", 3699.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(11L, "华为META10facebook", "手机", "华为", 4499.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(12L, "facebook小米Mix2S", "手机", "小米", 4299.00, "http://image.baidu.com/13123.jpg"));
        list.add(new Item(13L, "荣耀V10facebook", "手机", "华为", 2799.00, "http://image.baidu.com/13123.jpg"));
        // 接收对象集合，实现批量新增
        itemRepository.saveAll(list);
    }

    /**
      * @Description:matchQuery底层采用的是词条匹配查询
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:46
       */
    @Test
    public void testQuery(){

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米"));

        Page<Item> page = itemRepository.search(queryBuilder.build());

        long totalElements = page.getTotalElements();
        System.out.println("获取的总条数："+totalElements);

        for (Item item : page) {
            System.out.println(item);
        }


    }

    /**
      * @Description:词条匹配查询
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:45
       */
    @Test
    public void testTermQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.withQuery(QueryBuilders.termQuery("price",4499.0));

        Page<Item> page = itemRepository.search(queryBuilder.build());

        for (Item item : page) {
            System.out.println(item);
        }
    }

    /**
      * @Description:BooleanQuery
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:45
       */
    @Test
    public void testBooleanQuery(){

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.withQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title","华为"))
            .must(QueryBuilders.matchQuery("brand","华为"))
        );

        Page<Item> page = itemRepository.search(queryBuilder.build());

        for (Item item : page) {
            System.out.println(item);
        }

    }

    /**
      * @Description:模糊查询,最多错2次
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:23
       */
    @Test
    public void testFuzzyQuery(){

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.withQuery(QueryBuilders.fuzzyQuery("title","facebooo"));

        Page<Item> page = itemRepository.search(queryBuilder.build());

        for (Item item : page) {
            System.out.println(item);
        }

    }

    /**
      * @Description:es分页查询
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:29
       */
    @Test
    public void testPage(){

        int pageNum = 0;
        int pageSize = 11;

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.withQuery(QueryBuilders.matchQuery("category","手机"));

        queryBuilder.withPageable(PageRequest.of(pageNum, pageSize));

        Page<Item> page = itemRepository.search(queryBuilder.build());

        for (Item item : page) {
            System.out.println(item);
        }

    }


    /**
      * @Description:排序查询  分页没有起作用
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 0:34
       */
    @Test
    public void testOrder(){

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.withQuery(QueryBuilders.matchQuery("category","手机"));

        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));

        queryBuilder.withPageable(PageRequest.of(0,100));

        Page<Item> page = itemRepository.search(queryBuilder.build());

        for (Item item : page) {
            System.out.println(item);
        }


    }

    /**
      * @Description:以品牌分组，求组内情况
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 20:21
       */
    @Test
    public void testBucket(){

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.addAggregation(AggregationBuilders.terms("brandsName").field("brand"));

        Page<Item> page = itemRepository.search(queryBuilder.build());

        AggregatedPage<Item> aggregatedPage = (AggregatedPage<Item>) page;

        Aggregation aggregation = aggregatedPage.getAggregation("brandsName");

        StringTerms st = (StringTerms) aggregation;

        List<StringTerms.Bucket> buckets = st.getBuckets();

        for (StringTerms.Bucket sb : buckets) {
            System.out.println(sb.getKeyAsString()+":"+sb.getDocCount());
        }

    }

    /**
      * @Description:terms+avg分组和平均值计算
      * @Author: https://blog.csdn.net/chen_2890
      * @Date: 2018/9/29 21:06
       */
    @Test
    public void testMetric(){

        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();

        queryBuilder.addAggregation(AggregationBuilders.terms("brandKey").field("brand").subAggregation(AggregationBuilders.avg("avgPrice").field("price")));

        AggregatedPage<Item> aggregatedPage = (AggregatedPage<Item>) itemRepository.search(queryBuilder.build());

        StringTerms stringTerms = (StringTerms) aggregatedPage.getAggregation("brandKey");

        List<StringTerms.Bucket> buckets = stringTerms.getBuckets();

        for (StringTerms.Bucket bucket : buckets) {
            InternalAvg avgPrice = (InternalAvg) bucket.getAggregations().asMap().get("avgPrice");
            System.out.println(bucket.getKeyAsString()+":"+bucket.getDocCount()+":"+avgPrice.getValue());
        }


    }

    @Test
    public void testStudentCreateIndex() {
        elasticsearchTemplate.createIndex(Student.class);
    }

    @Test
    public void testFindByName(){

        List<Student> students = studentRepository.findByName("小名");
        for (Student s : students) {
            System.out.println(s.getName());
        }

    }

    @Test
    public void testInsertStudent() {
        List<Student> students = new ArrayList<Student>();
        students.add(new Student(1L,"小名",1,1));
        students.add(new Student(2L,"XIAO",1,1));
        students.add(new Student(3L,"之药",1,1));
        studentRepository.saveAll(students);
    }

}
