#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include "threadpool.h"
#include "DBPool.h"
using namespace std;

#define DB_HOST_IP          "127.0.0.1"             // 数据库服务器ip
#define DB_HOST_PORT        3306
#define DB_DATABASE_NAME    "mysql_pool_test"       // 数据库对应的库名字, 这里需要自己提前用命令创建完毕
#define DB_USERNAME         "root"                  // 数据库用户名
#define DB_PASSWORD         "123456"                // 数据库密码
#define DB_POOL_NAME        "mysql_pool"            // 连接池的名字，便于将多个连接池集中管理
#define DB_POOL_MAX_CON     4                       // 连接池支持的最大连接数量

// #define DB_HOST_IP          "114.215.169.66"             // 数据库服务器ip
// #define DB_HOST_PORT        3306
// #define DB_DATABASE_NAME    "mysql_pool_test"       // 数据库对应的库名字, 这里需要自己提前用命令创建完毕
// #define DB_USERNAME         "root"                  // 数据库用户名
// #define DB_PASSWORD         "01222t"                // 数据库密码
// #define DB_POOL_NAME        "mysql_pool"            // 连接池的名字，便于将多个连接池集中管理
// #define DB_POOL_MAX_CON     4                       // 连接池支持的最大连接数量

#define TASK_NUMBER 100

struct threadpool *threadpool_init(int thread_num, int queue_max_num)
{
    struct threadpool *pool = NULL;
    do
    {
        pool = (struct threadpool *)malloc(sizeof(struct threadpool));
        if (NULL == pool)
        {
            printf("failed to malloc threadpool!\n");
            break;
        }
        pool->thread_num = thread_num;
        pool->queue_max_num = queue_max_num;
        pool->queue_cur_num = 0;
        pool->head = NULL;
        pool->tail = NULL;
        if (pthread_mutex_init(&(pool->mutex), NULL))
        {
            printf("failed to init mutex!\n");
            break;
        }
        if (pthread_cond_init(&(pool->queue_empty), NULL))
        {
            printf("failed to init queue_empty!\n");
            break;
        }
        if (pthread_cond_init(&(pool->queue_not_empty), NULL))
        {
            printf("failed to init queue_not_empty!\n");
            break;
        }
        if (pthread_cond_init(&(pool->queue_not_full), NULL))
        {
            printf("failed to init queue_not_full!\n");
            break;
        }
        pool->pthreads = (pthread_t *)malloc(sizeof(pthread_t) * thread_num);
        if (NULL == pool->pthreads)
        {
            printf("failed to malloc pthreads!\n");
            break;
        }
        pool->queue_close = 0;
        pool->pool_close = 0;
        int i;
        for (i = 0; i < pool->thread_num; ++i)
        {
            pthread_create(&(pool->pthreads[i]), NULL, threadpool_function, (void *)pool);
        }

        return pool;
    } while (0);

    return NULL;
}

int threadpool_add_job(struct threadpool *pool, void *(*callback_function)(void *arg), void *arg)
{
    assert(pool != NULL);
    assert(callback_function != NULL);
    assert(arg != NULL);

    pthread_mutex_lock(&(pool->mutex));
    while ((pool->queue_cur_num == pool->queue_max_num) && !(pool->queue_close || pool->pool_close))
    {
        pthread_cond_wait(&(pool->queue_not_full), &(pool->mutex)); //队列满的时候就等待
    }
    if (pool->queue_close || pool->pool_close) //队列关闭或者线程池关闭就退出
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }
    struct job *pjob = (struct job *)malloc(sizeof(struct job));
    if (NULL == pjob)
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }
    pjob->callback_function = callback_function;
    pjob->arg = arg;
    pjob->next = NULL;
    if (pool->head == NULL)
    {
        pool->head = pool->tail = pjob;
        pthread_cond_broadcast(&(pool->queue_not_empty)); //队列空的时候，有任务来时就通知线程池中的线程：队列非空
    }
    else
    {
        pool->tail->next = pjob;
        pool->tail = pjob;
    }
    pool->queue_cur_num++;
    pthread_mutex_unlock(&(pool->mutex));
    return 0;
}

static uint64_t get_tick_count()
{
    struct timeval tval;
    uint64_t ret_tick;

    gettimeofday(&tval, NULL);

    ret_tick = tval.tv_sec * 1000L + tval.tv_usec / 1000L;
    return ret_tick;
}

void *threadpool_function(void *arg)
{
    struct threadpool *pool = (struct threadpool *)arg;
    struct job *pjob = NULL;
    uint64_t start_time = get_tick_count();
    uint64_t end_time = get_tick_count();
    while (1) //死循环
    {
        pthread_mutex_lock(&(pool->mutex));
        while ((pool->queue_cur_num == 0) && !pool->pool_close) //队列为空时，就等待队列非空
        {
            end_time = get_tick_count();    // 没有任务的时候设置读取最后处理任务的时间
            printf("threadpool need time:%lums\n", end_time - start_time);
            pthread_cond_wait(&(pool->queue_not_empty), &(pool->mutex));
        }
        if (pool->pool_close) //线程池关闭，线程就退出
        {
            pthread_mutex_unlock(&(pool->mutex));
            pthread_exit(NULL);
        }
        pool->queue_cur_num--;
        pjob = pool->head;
        if (pool->queue_cur_num == 0)
        {
            pool->head = pool->tail = NULL;
        }
        else
        {
            pool->head = pjob->next;
        }
        if (pool->queue_cur_num == 0)
        {
            pthread_cond_signal(&(pool->queue_empty)); //队列为空，就可以通知threadpool_destroy函数，销毁线程函数
        }
        if (pool->queue_cur_num == pool->queue_max_num - 1)
        {
            pthread_cond_broadcast(&(pool->queue_not_full)); //队列非满，就可以通知threadpool_add_job函数，添加新任务
        }
        pthread_mutex_unlock(&(pool->mutex));

        (*(pjob->callback_function))(pjob->arg); //线程真正要做的工作，回调函数的调用
        free(pjob);
        pjob = NULL;
    }
}
int threadpool_destroy(struct threadpool *pool)
{
    assert(pool != NULL);
    pthread_mutex_lock(&(pool->mutex));
    if (pool->queue_close || pool->pool_close) //线程池已经退出了，就直接返回
    {
        pthread_mutex_unlock(&(pool->mutex));
        return -1;
    }

    pool->queue_close = 1; //置队列关闭标志
    while (pool->queue_cur_num != 0)
    {
        pthread_cond_wait(&(pool->queue_empty), &(pool->mutex)); //等待队列为空
    }

    pool->pool_close = 1; //置线程池关闭标志
    pthread_mutex_unlock(&(pool->mutex));
    pthread_cond_broadcast(&(pool->queue_not_empty)); //唤醒线程池中正在阻塞的线程
    pthread_cond_broadcast(&(pool->queue_not_full));  //唤醒添加任务的threadpool_add_job函数
    int i;
    for (i = 0; i < pool->thread_num; ++i)
    {
        pthread_join(pool->pthreads[i], NULL); //等待线程池的所有线程执行完毕
    }

    pthread_mutex_destroy(&(pool->mutex)); //清理资源
    pthread_cond_destroy(&(pool->queue_empty));
    pthread_cond_destroy(&(pool->queue_not_empty));
    pthread_cond_destroy(&(pool->queue_not_full));
    free(pool->pthreads);
    struct job *p;
    while (pool->head != NULL)
    {
        p = pool->head;
        pool->head = p->next;
        free(p);
    }
    free(pool);
    return 0;
}



// #define random(x) (rand()%x)

static string int2string(uint32_t user_id)
{
    stringstream ss;
    ss << user_id;
    return ss.str();
}

#define DROP_IMUSER_TABLE "DROP TABLE IF EXISTS IMUser" /* if EXISTS 好处 是如果表不存在,执行不会报错 */
                                                        

#define CREATE_IMUSER_TABLE "CREATE TABLE IMUser (     \
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '用户id',   \
  `sex` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '1男2女0未知', \
  `name` varchar(32) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '用户名',  \
  `domain` varchar(32) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '拼音',  \
  `nick` varchar(32) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '花名,绰号等', \
  `password` varchar(32) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '密码',    \
  `salt` varchar(4) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '混淆码',   \
  `phone` varchar(11) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '手机号码',   \
  `email` varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT 'email',  \
  `company` varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '公司名称', \
  `address` varchar(64) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '所在地区', \
  `avatar` varchar(255) COLLATE utf8mb4_bin DEFAULT '' COMMENT '自定义用户头像',    \
  `validateMethod` tinyint(2) unsigned DEFAULT '1' COMMENT '好友验证方式',  \
  `departId` int(11) unsigned NOT NULL DEFAULT '1' COMMENT '所属部门Id',    \
  `status` tinyint(2) unsigned DEFAULT '0' COMMENT '1. 试用期 2. 正式 3. 离职 4.实习',  \
  `created` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '创建时间',   \
  `updated` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '更新时间',   \
  `push_shield_status` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '0关闭勿扰 1开启勿扰',  \
  `sign_info` varchar(128) COLLATE utf8mb4_bin NOT NULL DEFAULT '' COMMENT '个性签名',  \
  PRIMARY KEY (`id`),   \
  KEY `idx_domain` (`domain`),  \
  KEY `idx_name` (`name`),  \
  KEY `idx_phone` (`phone`) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

#define INSERT_SAMPLE "INSERT INTO user(name,email,phone) VALUES(?,?,?)"
#define SELECT_SAMPLE "SELECT name,email,phone FROM user"

//1.创建数据库mysql_pool_test:  create database mysql_pool_test;
// show databases;    查看数据库
// show tables;       查看有哪些表
// desc table_name;   查看表结构
//

typedef unsigned int atomic_uint;

static atomic_uint IMUser_nId = 0;

// 初始化原子变量
#define atomic_init(obj, value) \
    do                          \
    {                           \
        *(obj) = (value);       \
    } while (0)

// 原子变量加操作
#define atomic_fetch_add(object, operand) \
    __sync_fetch_and_add(object, operand)
// 原子变量减操作
#define atomic_fetch_sub(object, operand) \
    __sync_fetch_and_sub(object, operand)

bool insertUser(CDBConn *pDBConn)
{
    bool bRet = false;
    string strSql;
    strSql = "insert into IMUser(`salt`,`sex`,`nick`,`password`,`domain`,`name`,`phone`,`email`,`company`,`address`,`avatar`,`sign_info`,`departId`,`status`,`created`,`updated`) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    atomic_fetch_add(&IMUser_nId, 1);
    int id_index = IMUser_nId;
    
    CPrepareStatement *stmt = new CPrepareStatement();
    if (stmt->Init(pDBConn->GetMysql(), strSql))
    {
        uint32_t nNow = (uint32_t)time(NULL);
        uint32_t index = 0;
        string strOutPass = "987654321";
        string strSalt = "abcd";

        int nSex = 1;                             // 用户性别 1.男;2.女
        int nStatus = 0;                          // 用户状态0 正常， 1 离职
        uint32_t nDeptId = 0;                     // 所属部门
        string strNick = "明华";                  // 花名
        string strDomain = "minghua";             // 花名拼音
        string strName = "廖庆富";                // 真名
        string strTel = "1857036";            // 手机号码
        string strEmail = "326873713@qq.com";     // Email
        string strAvatar = "";                    // 头像
        string sign_info = "一切只为你";          //个性签名
        string strPass = "123456";                //密码
        string strCompany = "零声学院";           //公司
        string strAddress = "长沙岳麓区雅阁国际"; //地址

        stmt->SetParam(index++, strSalt);
        stmt->SetParam(index++, nSex);
        stmt->SetParam(index++, strNick);
        stmt->SetParam(index++, strOutPass);
        stmt->SetParam(index++, (strDomain + int2string(id_index)));
        stmt->SetParam(index++, (strName + int2string(id_index)));
        stmt->SetParam(index++, (strTel+ int2string(id_index)));
        stmt->SetParam(index++, strEmail);
        stmt->SetParam(index++, strCompany);
        stmt->SetParam(index++, strAddress);
        stmt->SetParam(index++, strAvatar);
        stmt->SetParam(index++, sign_info);
        stmt->SetParam(index++, nDeptId);
        stmt->SetParam(index++, nStatus);
        stmt->SetParam(index++, nNow);
        stmt->SetParam(index++, nNow);
        bRet = stmt->ExecuteUpdate();

        if (!bRet)
        {
            printf("insert user failed: %s\n", strSql.c_str());
        }
        else
        {
            uint32_t nId = (uint32_t)stmt->GetInsertId();
            // printf("register then get user_id:%d\n", nId);
        }
    }
    delete stmt;

    return true;
}
// 使用连接池的方式
void *workUsePool(void *arg)
{
    CDBPool *pDBPool = (CDBPool *)arg;
    CDBConn *pDBConn = pDBPool->GetDBConn(2000);        // 取连接
    if (pDBConn)
    {
        bool ret = insertUser(pDBConn);
        if (!ret)
        {
            printf("insertUser failed\n");
        }
    }
    else
    {
        printf("GetDBConn failed\n");
    }
    pDBPool->RelDBConn(pDBConn);        // 然后释放连接
    return NULL;
}
// 没有用连接池
void *workNoPool(void *arg)
{
    // printf("workNoPool\n");
    const char *db_pool_name    = DB_POOL_NAME;
    const char* db_host         = DB_HOST_IP;
    int   db_port               = DB_HOST_PORT;
    const char* db_dbname       = DB_DATABASE_NAME;
    const char* db_username     = DB_USERNAME;
    const char* db_password     = DB_PASSWORD;

    int db_maxconncnt = 1;

    CDBPool *pDBPool = new CDBPool(db_pool_name, db_host, db_port,
                                   db_username, db_password, db_dbname, db_maxconncnt);
    if (!pDBPool)
    {
        printf("workNoPool new CDBPool failed\n");
        return NULL;
    }
    if (pDBPool->Init())
    {
        printf("init db instance failed: %s\n", db_pool_name);
        return NULL;
    }

    CDBConn *pDBConn = pDBPool->GetDBConn();
    if (pDBConn)
    {
        bool ret = insertUser(pDBConn);
        if (!ret)
        {
            printf("insertUser failed\n");
        }
    }
    else
    {
        printf("GetDBConn failed\n");
    }
    pDBPool->RelDBConn(pDBConn);
    delete pDBPool;
    return NULL;
}



// 查看最大连接数 show variables like '%max_connections%';
// 修改最大连接数 set GLOBAL max_connections = 200;
// 如果是root帐号，你能看到所有用户的当前连接。如果是其它普通帐号，只能看到自己占用的连接。
// 命令： show processlist;
int main(int argc, char *argv[])
{
    int thread_num = 1;    // 线程池线程数量
    int db_maxconncnt = DB_POOL_MAX_CON; // 连接池最大连接数量(核数*2 + 磁盘数量)
    int use_pool = 1;      // 是否使用连接池
    if (argc != 4)
    {
        printf("usage:  ./test_ThreadPool thread_num db_maxconncnt use_pool\n \
                example: ./test_ThreadPool 4  4 1\n");

        return 1;
    }
    thread_num = atoi(argv[1]);
    db_maxconncnt = atoi(argv[2]);
    use_pool = atoi(argv[3]);

    const char *db_pool_name    = DB_POOL_NAME;
    const char* db_host         = DB_HOST_IP;
    int   db_port               = DB_HOST_PORT;
    const char* db_dbname       = DB_DATABASE_NAME;
    const char* db_username     = DB_USERNAME;
    const char* db_password     = DB_PASSWORD;


    // 每个连接池都对应一个对象
    CDBPool *pDBPool = new CDBPool(db_pool_name, db_host, db_port,
                                   db_username, db_password, db_dbname, db_maxconncnt);

    if (pDBPool->Init())
    {
        printf("init db instance failed: %s", db_pool_name);
        return 1;
    }

    CDBConn *pDBConn = pDBPool->GetDBConn();                // 获取连接
    if (pDBConn)
    {
        bool ret = pDBConn->ExecuteDrop(DROP_IMUSER_TABLE); // 删除表
        if (ret)
        {
            printf("DROP_IMUSER_TABLE ok\n");
        }
        // 1. 创建表
        ret = pDBConn->ExecuteCreate(CREATE_IMUSER_TABLE);  // 创建表
        if (ret)
        {
            printf("CREATE_IMUSER_TABLE ok\n");
        }
        else
        {
            printf("ExecuteCreate failed\n");
            return -1;
        }
    }
    pDBPool->RelDBConn(pDBConn);

    printf("thread_num = %d, connection_num:%d, use_pool:%d\n", 
        thread_num, db_maxconncnt, use_pool);
    struct threadpool *pool = threadpool_init(thread_num, TASK_NUMBER); // 初始化线程
    for (int i = 0; i < TASK_NUMBER; i++)
    {
        if (use_pool)       // 使用连接池时任务采用workUsePool
        {
            threadpool_add_job(pool, workUsePool, (void *)pDBPool);
        }
        else                // 不使用连接池时任务采用workNoPool
        {
            threadpool_add_job(pool, workNoPool, (void *)pDBPool);
        }
    }

    while(pool->queue_cur_num !=0 )     // 判断队列是否还有任务
    {
         sleep(1);  // 还有任务主线程继续休眠
    }
    sleep(2);       // 没有任务要处理了休眠2秒退出，这里等待主要是确保所有线程都已经空闲
    threadpool_destroy(pool);

    delete pDBPool;
    return 0;
}