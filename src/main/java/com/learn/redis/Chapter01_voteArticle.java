package com.learn.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01_voteArticle {
    private static final String IP = "192.168.56.102";

    private static final int PORT = 6379;

    private static final int SECONDS_IN_ONE_WEEK = 7 * 86400;

    private static final int VOTE_SCORE = 432;// 文章评分增量

    private static final int ARTICLES_PER_PAGE = 10;// 每页10条数据

    public static final void main(String[] args) {
        new Chapter01_voteArticle().test();
    }

    public void test() {
        Jedis conn = new Jedis(IP, PORT);
        conn.select(15);// 测试是否连接上

        while (true) {
            postArticleTest(conn);
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
        }
        // getArticleData(conn, "1");
        // voteTest(conn, "2","username"+new Random().nextInt(100));
        // getTop10Article(conn);
        // addGroupsTest(conn, "10", new String[]{"A"});
        // getGroupArticles(conn, "A");

    }

    public void postArticleTest(Jedis conn) {
        String articleId = postArticle(conn, "张三", "新闻1", "http://www.google.com");
//        System.out.println("发布了一篇新的文章 ：" + articleId);
//        System.out.println("其具体信息如下：");
//        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
//        for (Map.Entry<String, String> entry : articleData.entrySet()) {
//            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
//        }
    }

    public void getArticleData(Jedis conn, String articleId) {
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
    }

    public void voteTest(Jedis conn, String articleId, String username) {
        // 投票
        articleVote(conn, username, "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("投了一票，现在的票数是： " + votes);
        assert Integer.parseInt(votes) > 1;
    }

    private void getTop10Article(Jedis conn) {
        // 获取评分最高的文章
        System.out.println("当前评分最高的文章是(前10条)：");
        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    private void getGroupArticles(Jedis conn, String group) {
        // 获取分组文章
        List<Map<String, String>> articles = getGroupArticles(conn, group, 1);
        printArticles(articles);
    }

    /**
     * 发布文章 vote：投票 [vəʊt]
     * 
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        // 发布一篇文章首先需要创建一个新的文章id，这项工作可以对一个计数器 （counter）执行incr命令完成
        String articleId = String.valueOf(conn.incr("article:"));

        String voted = "voted:" + articleId;// 文章已投票用户的集合，键名称
        // 将给定元素添加到集合（将该用户添加到文章已投票用户集合中）
        conn.sadd(voted, user);
        // 设置文章的过期投票时间
        conn.expire(voted, SECONDS_IN_ONE_WEEK);

        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;// 记录文章信息的散列，键名称
        HashMap<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");// 投票数初始值为1
        conn.hmset(article, articleData);// 一个文章的信息存储于一个散列中
        // 将一个带有给定分值的成员添加到有序集合里面（存储评分信息的有序集合）
        conn.zadd("score:", now + VOTE_SCORE, article);
        // 存储发布时间的有序集合
        conn.zadd("time:", now, article);

        return articleId;
    }

    /**
     * 文章投票
     * 
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article) {
        // 取出文章的发布时间
        Double postTime = conn.zscore("time:", article);
        if ((System.currentTimeMillis() / 1000 - postTime) > SECONDS_IN_ONE_WEEK) {
            return;// 大于一周，停止投票
        }
        String articleId = article.substring(article.indexOf(':') + 1);
        // 如果添加成功，说明投票有效
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:", VOTE_SCORE, article);// 评分增加
            conn.hincrBy(article, "votes", 1);// 文章信息更新，票数加1
        }
    }

    private void addGroupsTest(Jedis conn, String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    /**
     * 根据分组获取文章
     * 
     * @param conn
     * @param group
     * @param page
     * @return
     */
    private List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    private List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;
        if (!conn.exists(key)) {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }

    private List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    private List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids) {
            Map<String, String> articleDataMap = conn.hgetAll(id);
            articleDataMap.put("id", id);
            articles.add(articleDataMap);
        }

        return articles;
    }

    /**
     * 打印文章信息
     * 
     * @param articles
     */
    private void printArticles(List<Map<String, String>> articles) {
        for (Map<String, String> article : articles) {
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String, String> entry : article.entrySet()) {
                if (entry.getKey().equals("id")) {
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
