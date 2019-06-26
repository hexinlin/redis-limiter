package com.hxl.redis_limiter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.log4j.Logger;
/**
 * 
 * @author hexinlin
 *
 */

public class RedisLimiter {

	private static Long _frequency = null;
	private static Integer _number = null;
	private static Properties _prop = null;
	private final static Logger _logger = Logger.getLogger(RedisLimiter.class);
	static {
		try {
		InputStream inputStream = RedisLimiter.class.getClassLoader().getResourceAsStream("limiter.properties");
		_prop = new Properties();
		_prop.load(inputStream);
		_frequency = Long.parseLong(_prop.getProperty("frequency"));
		_number = Integer.parseInt(_prop.getProperty("number"));
		} catch (IOException e) {
			_logger.error(e);
		}
		
	}
	
	/**
	 * 获取限流通过权限
	 * @param limiterName 限流器名称，不同名称对应不同限流器
	 * @return
	 */
	public static boolean getRequestPermissions(String limiterName) {
		InterProcessMutex ipm = ZookeeperClient.acquire(limiterName);
		RedisUtil redis = RedisUtil.getRedisUtil();
		long length = redis.llen(limiterName);
		boolean flag = false;
		if(length<_number) {
			redis.lpush(limiterName, System.nanoTime()+"");
			flag = true;
			
		}else {
			String lastItem =redis.lrange(limiterName, length-1, length).get(0);
			if(System.nanoTime()-Long.parseLong(lastItem)>(_frequency*1000000)) {
				redis.lpush(limiterName, System.nanoTime()+"");
				redis.rpop(limiterName);
				flag = true;
				
			}else {
				flag = false;
			}
		}
		try {
			ipm.release();
		} catch (Exception e) {
			_logger.error(e);
		}
		return flag;
	}
	
	
	public static void main(String[] args) throws Exception{
		final String limiterName = "limiter-0";
		//限流器测试
		for(;;) {
			new Thread(new Runnable() {
				
				public void run() {
					System.out.println("Thread:"+Thread.currentThread().getId()+",获取限流权限结果："+RedisLimiter.getRequestPermissions(limiterName));
				}
			}).start();
			
			TimeUnit.MILLISECONDS.sleep(500);
		}
	}
	
}
