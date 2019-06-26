package com.hxl.redis_limiter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

public class ZookeeperClient {

	private static String _zkUrl = null;
	private static CuratorFramework _client = null;
	private static Properties _prop =null;
	private static String _zkBaseDir = null;
	private static final Logger _logger = Logger.getLogger(ZookeeperClient.class);
	
	
	
	static {
		
		try {
			InputStream inputStream = RedisUtil.class.getClassLoader().getResourceAsStream("zk.properties");
			_prop = new Properties();
			_prop.load(inputStream);
			_zkUrl = _prop.getProperty("zkUrl");
			_zkBaseDir = _prop.getProperty("zkBaseDir");
		} catch (IOException e) {
			_logger.error(e);
		}
		
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		_client = CuratorFrameworkFactory.newClient(_zkUrl, retryPolicy);
		_client.start();
	}
	
	/**
	 * 获取分布式锁
	 * @param lockObj
	 * @return 返回锁对象
	 */
	public static InterProcessMutex acquire(String lockObj) {
		InterProcessMutex ipm = new InterProcessMutex(_client, _zkBaseDir+"/"+lockObj);
		try {
			ipm.acquire();
		} catch (Exception e) {
			_logger.error(e);
			try {
				ipm.release();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return ipm;
	}
	
	
}
