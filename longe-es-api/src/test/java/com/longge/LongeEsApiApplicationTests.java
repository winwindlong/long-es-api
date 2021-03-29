package com.longge;

import com.alibaba.fastjson.JSON;
import com.longge.config.ESConstant;
import com.longge.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class LongeEsApiApplicationTests {

	@Autowired
	private RestHighLevelClient restHighLevelClient;

	//测试索引的创建
	@Test
	void testCreateIndex() throws IOException {
		CreateIndexRequest request = new CreateIndexRequest("long_index");
		CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
		System.out.println(createIndexResponse);
	}

	//测试获取索引,只能判读是否存在
	@Test
	void testExistIndex() throws IOException {
		GetIndexRequest index = new GetIndexRequest("long_index");
		boolean exists = restHighLevelClient.indices().exists(index, RequestOptions.DEFAULT);
		System.out.println(exists);
	}
	//测试删除索引
	@Test
	void testDeleteIndex() throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest("long_index");
		AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
		System.out.println(response);
	}

	//测试添加文档
	@Test
	void testAddDocument() throws IOException {
		User user = new User("jlzhang", 18);
		IndexRequest request = new IndexRequest("long_index");
		request.id("11");
		request.timeout(TimeValue.timeValueSeconds(1));
		request.source(JSON.toJSONString(user), XContentType.JSON);
		IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
		System.out.println(indexResponse.toString());
		System.out.println(indexResponse.status());
	}

	/**
	 * 获得文档的信息
	 * @throws IOException
	 */
	@Test
	void testExists() throws IOException {
		GetRequest getRequest = new GetRequest("long_index", "1");
		//不获取返回的_source上下文
		getRequest.fetchSourceContext(new FetchSourceContext(false));
		getRequest.storedFields("_none");
		boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	@Test
	void testGetDocument() throws IOException {
		GetRequest getRequest = new GetRequest("long_index", "1");
		GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
		System.out.println(response.getSourceAsString());
		System.out.println(response);
	}

	/**
	 * 更新文档
	 * @throws IOException
	 */
	@Test
	void testUpdateDocument() throws IOException {
		UpdateRequest request = new UpdateRequest("long_index", "1");
		request.timeout("1s");
		User user = new User("章江龙 say java", 20);
		request.doc(JSON.toJSONString(user), XContentType.JSON);
		UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);
		System.out.println(updateResponse.status());
	}

	/**
	 * 删除文档
	 * @throws IOException
	 */
	@Test
	void testDeleteDoc() throws IOException {
		DeleteRequest request = new DeleteRequest("long_index", "1");
		request.timeout("1s");
		DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
		System.out.println(deleteResponse.status());
	}

	/**
	 * 批量插入数据
	 */
	@Test
	void testBulkRequest() throws IOException {
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s");
		ArrayList<Object> arrayList = new ArrayList<>();
		arrayList.add(new User("章江龙",1));
		arrayList.add(new User("章江龙2",2));
		arrayList.add(new User("章江龙3",3));
		arrayList.add(new User("章江龙4",4));
		arrayList.add(new User("章江龙5",5));
		arrayList.add(new User("章江龙6",6));
		arrayList.add(new User("章江龙7",7));
		arrayList.add(new User("章江龙8",8));
		for (int i = 0; i < arrayList.size(); i++) {
			bulkRequest.add(new IndexRequest("long_index").id("" + (i+1)).source(JSON.toJSONString(arrayList.get(i)), XContentType.JSON));
		}
		BulkResponse responses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(responses.status());
	}

	@Test
	void testSearch() throws IOException {
		SearchRequest searchRequest = new SearchRequest("long_index");
		SearchSourceBuilder builder = new SearchSourceBuilder();
/*		QueryBuilders.matchAllQuery();//匹配所有
		QueryBuilders.termQuery();//精确匹配*/
		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", "章江龙");
		builder.query(termQueryBuilder);
		builder.timeout(new TimeValue(60, TimeUnit.SECONDS));
		searchRequest.source(builder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
		System.out.println(JSON.toJSONString(searchResponse.getHits()));
		System.out.println("==============================");
		for (SearchHit hit : searchResponse.getHits().getHits()) {
			System.out.println(hit.getSourceAsMap());
		}
	}
	@Test
	void contextLoads() {

	}

}
