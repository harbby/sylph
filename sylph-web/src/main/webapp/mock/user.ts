import { Request, Response } from 'express';

const waitTime = (time: number = 100) => {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(true);
    }, time);
  });
};

async function getFakeCaptcha(req: Request, res: Response) {
  await waitTime(2000);
  return res.json('captcha-xxx');
}

const { ANT_DESIGN_PRO_ONLY_DO_NOT_USE_IN_YOUR_PRODUCTION } = process.env;

/**
 * 当前用户的权限，如果为空代表没登录
 * current user access， if is '', user need login
 * 如果是 pro 的预览，默认是有权限的
 */
let access = ANT_DESIGN_PRO_ONLY_DO_NOT_USE_IN_YOUR_PRODUCTION === 'site' ? 'admin' : '';

const getAccess = () => {
  return access;
};

// 代码中会兼容本地 service mock 以及部署站点的静态数据
export default {
  // 支持值为 Object 和 Array
  'GET /api/currentUser': (req: Request, res: Response) => {
    if (!getAccess()) {
      res.status(401).send({
        data: {
          isLogin: false,
        },
        errorCode: '401',
        errorMessage: '请先登录！',
        success: true,
      });
      return;
    }
    res.send({
      name: 'Qiang Ma',
      avatar: 'https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png',
      userid: '00000001',
      email: 'antdesign@alipay.com',
      signature: '海纳百川，有容乃大',
      title: '交互专家',
      group: '蚂蚁金服－某某某事业群－某某平台部－某某技术部－UED',
      tags: [
        {
          key: '0',
          label: '很有想法的',
        },
        {
          key: '1',
          label: '专注设计',
        },
        {
          key: '2',
          label: '辣~',
        },
        {
          key: '3',
          label: '大长腿',
        },
        {
          key: '4',
          label: '川妹子',
        },
        {
          key: '5',
          label: '海纳百川',
        },
      ],
      notifyCount: 12,
      unreadCount: 11,
      country: 'China',
      access: getAccess(),
      geographic: {
        province: {
          label: '浙江省',
          key: '330000',
        },
        city: {
          label: '杭州市',
          key: '330100',
        },
      },
      address: '西湖区工专路 77 号',
      phone: '0752-268888888',
    });
  },
  // GET POST 可省略
  'GET /_sys/job_manger/jobs': [{"jobName":"json","queryText":"-- 本例子测试 json kafka message\ncreate function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';\n\ncreate source table topic1(\n    ip varchar,\n    heartbeat_type varchar,\n    user_id varchar,\n    mac varchar,\n    server_time bigint\n) with (\n    type = 'kafka',\n    kafka_topic = 'test1,test2',\n    auto.offset.reset = latest,\n    kafka_broker = 'localhost:9092',\n    kafka_group_id = 'streamSql_test11',\n  \tzookeeper.connect = 'localhost:2181',\n    value_type = 'json'\n);\n\n-- 定义数据流输出位置\ncreate sink table print_table_sink(\n    ip varchar,\n    heartbeat_type varchar,\n    user_id varchar,\n    mac varchar,\n    server_time bigint\n) with (\n    type = 'console',   -- print console\n    other = 'demo001'\n);\n\n\n-- 描述数据流计算过程\ninsert into print_table_sink\nselect * from topic1","type":"StreamSql","config":"{\"taskManagerMemoryMb\":1024,\"taskManagerCount\":2,\"taskManagerSlots\":2,\"jobManagerMemoryMb\":1024,\"checkpointInterval\":-1,\"checkpointTimeout\":600000,\"checkpointDir\":\"hdfs:///tmp/sylph/flink/savepoints/\",\"minPauseBetweenCheckpoints\":0,\"parallelism\":4,\"queue\":\"default\",\"appTags\":[\"Sylph\",\"Flink\"],\"enableSavepoint\":false}","description":null,"jobId":2,"status":"STARTED_ERROR","runId":"","appUrl":"/proxy/2/#"},{"jobName":"join_test","queryText":"-- 本例子测试 如何数据源带有event_time 可直接设置 WATERMARK\ncreate function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';\n\ncreate source table topic1(\n    key varchar,\n    message varchar,     -- json\n    event_time bigint,\n    proctime as proctime()\n) with (\n    type = 'test'\n);\n\n-- 定义数据流输出位置\ncreate sink table print_table_sink(\n    uid varchar,\n    name varchar,\n    city varchar,\n    cnt long,\n    window_time varchar\n) with (\n    type = 'console',   -- print console\n    other = 'demo001'\n);\n\n-- 定义维表\ncreate batch table users(\n    id varchar,\n    name varchar,\n    city varchar\n) with (\n    type = 'mysql',   -- or ideal.sylph.plugins.mysql.MysqlAsyncJoin\n    userName = 'demo',\n    password = 'demo',\n    url = 'jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false'\n    -- query = 'select * from users where ...'  --可以下推谓词\n);\n\n-- 描述数据流计算过程\ninsert into print_table_sink\nwith tb1 as (\n    select key, get_json_object(message,'user_id') as uid , get_json_object(message,'ip') as ip, event_time, proctime\n    from topic1\n),tb2 as (\n    select uid,\n    count(distinct key) as cnt,\n    cast(TUMBLE_START(proctime,INTERVAL '5' SECOND) as varchar)|| '-->' \n    || cast(TUMBLE_END(proctime,INTERVAL '5' SECOND) as varchar) AS start_time\n    from tb1 where uid is not null\n    group by uid,TUMBLE(proctime,INTERVAL '5' SECOND)\n) \nselect tb2.uid, users.name ,users.city, tb2.cnt, tb2.start_time \nfrom tb2 left join users on tb2.uid = users.id\nhaving 1=1","type":"StreamSql","config":"{\"taskManagerMemoryMb\":1024,\"taskManagerCount\":2,\"taskManagerSlots\":2,\"jobManagerMemoryMb\":1024,\"checkpointInterval\":10000,\"checkpointTimeout\":600000,\"checkpointDir\":\"hdfs:///tmp/sylph/flink/savepoints/\",\"minPauseBetweenCheckpoints\":0,\"parallelism\":4,\"queue\":\"default\",\"appTags\":[\"sylph\",\"flink\"],\"enableSavepoint\":false}","description":null,"jobId":4,"status":"STOP","runId":null,"appUrl":null},{"jobName":"sparksql_demo1","queryText":"create source table kafka(\n    _topic varchar,\n    _key varchar,\n    _partition integer,\n    _offset bigint,\n    _message varchar,\n    properties map<string,string>,\n    project varchar,\n    distinct_id varchar,\n    user_id bigint,\n    `time` bigint,\n    event varchar\n) with (\n    type = 'kafka',\n    kafka_topic = 'test1',\n    auto.offset.reset = latest,\n    kafka_broker = 'localhost:9092',\n    kafka_group_id = 'test1',\n    zookeeper.connect = 'localhost:2181',\n    auto.commit.enable = 'true',\n    auto.commit.interval.ms = '90000',\n    value_type = 'json'\n);\n\nselect * from kafka;\n","type":"StructuredStreamingSql","config":"{\"driver-memory\":\"1024m\",\"driver-cores\":1,\"num-executors\":2,\"executor-memory\":\"1600m\",\"executor-cores\":2,\"sparkStreamingBatchDuration\":5000,\"queue\":\"default\"}","description":null,"jobId":5,"status":"STOP","runId":null,"appUrl":null},{"jobName":"sql_test1","queryText":"create function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';\n\ncreate source table topic1(\n    _topic varchar,\n    _key varchar,\n    _message varchar,\n    _partition integer,\n    _offset bigint\n) with (\n    type = 'kafka09',\n    kafka_topic = 'test1,test2',\n    auto.offset.reset = latest,\n    kafka_broker = 'localhost:9092',\n    kafka_group_id = 'streamSql_test11',\n  \t\"zookeeper.connect\" = 'localhost:2181'\n);\n-- 定义数据流输出位置\ncreate sink table mysql_table_sink(\n    a1 varchar,\n    a2 varchar,\n    event_time bigint\n) with (\n    type = 'mysql',   -- or ideal.sylph.plugins.mysql.MysqlSink\n    userName = 'demo',\n    password = 'demo',\n    url = 'jdbc:mysql://localhost:3306/pop?characterEncoding=utf-8&useSSL=false',\n    query = 'insert into mysql_table_sink values(${0},${1},${2})'\n);\n-- 描述数据流计算过程\ninsert into mysql_table_sink\nselect _topic,`_message`,cast(_offset as bigint)\nfrom topic1 where _key is not null","type":"StreamSql","config":"{\"taskManagerMemoryMb\":1024,\"taskManagerCount\":2,\"taskManagerSlots\":1,\"jobManagerMemoryMb\":1024,\"checkpointInterval\":-1,\"checkpointTimeout\":600000,\"checkpointDir\":\"hdfs:///tmp/sylph/flink/savepoints/\",\"minPauseBetweenCheckpoints\":0,\"parallelism\":2,\"queue\":\"default\",\"appTags\":[\"demo1\",\"demo2\"],\"enableSavepoint\":false}","description":null,"jobId":6,"status":"STOP","runId":null,"appUrl":null},{"jobName":"streamSql_demo","queryText":"create function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';\n\ncreate source table topic1(\n    key varchar,\n    message varchar,\n    event_time bigint\n) with (\n    type = 'test'\n);\n\n-- 定义数据流输出位置\ncreate sink table print_table_sink(\n    key varchar,\n    cnt long,\n    window_time varchar\n) with (\n    type = 'console',   -- print console\n    other = 'demo001'\n);\n\n-- 定义 WATERMARK,通常您应该从kafka message中解析出event_time字段\ncreate view TABLE foo\nWATERMARK event_time FOR rowtime BY ROWMAX_OFFSET(5000)  --event_time 为您的真实数据产生时间\nAS\nwith tb1 as (select * from topic1)  --通常这里解析kafka message\nselect * from tb1;\n\n-- 描述数据流计算过程\ninsert into print_table_sink\nwith tb2 as (\n    select key,\n    count(1),\n    cast(TUMBLE_START(rowtime,INTERVAL '5' SECOND) as varchar)|| '-->'\n    || cast(TUMBLE_END(rowtime,INTERVAL '5' SECOND) as varchar) AS window_time\n    from foo where key is not null\n    group by key,TUMBLE(rowtime,INTERVAL '5' SECOND)\n) select * from tb2","type":"StreamSql","config":"{\"taskManagerMemoryMb\":1024,\"taskManagerCount\":2,\"taskManagerSlots\":2,\"jobManagerMemoryMb\":1024,\"checkpointInterval\":-1,\"checkpointTimeout\":600000,\"checkpointDir\":\"hdfs:///tmp/sylph/flink/savepoints/\",\"minPauseBetweenCheckpoints\":0,\"parallelism\":4,\"queue\":\"default\",\"appTags\":[\"demo1\",\"demo2\"],\"enableSavepoint\":false}","description":null,"jobId":7,"status":"STOP","runId":null,"appUrl":null},{"jobName":"hdfs_test","queryText":"create function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';\n\ncreate source table topic1(\n    key varchar,\n    message varchar,\n    event_time bigint\n) with (\n    type = 'test'\n);\n\n-- 定义数据流输出位置\ncreate sink table event_log(\n    key varchar,\n    message varchar,\n    event_time bigint\n) with (\n    type = 'hdfs',   -- print console\n    hdfs_write_dir = 'hdfs:///tmp/test/data/xx_log',\n    eventTime_field = 'event_time', --哪个字段是event_time\n    format = 'parquet'\n);\n\ninsert into event_log\nselect key,message,event_time from topic1","type":"StreamSql","config":"{\"taskManagerMemoryMb\":2048,\"taskManagerCount\":2,\"taskManagerSlots\":2,\"jobManagerMemoryMb\":1024,\"checkpointInterval\":-1,\"checkpointTimeout\":600000,\"checkpointDir\":\"hdfs:///tmp/sylph/flink/savepoints/\",\"minPauseBetweenCheckpoints\":0,\"parallelism\":4,\"queue\":\"default\",\"appTags\":[\"Sylph\",\"Flink\"],\"enableSavepoint\":false}","description":null,"jobId":8,"status":"STOP","runId":null,"appUrl":null},{"jobName":"streamSql_demo2","queryText":"-- 本例子测试 如何数据源带有event_time 直接设置 WATERMARK\ncreate function get_json_object as 'ideal.sylph.runner.flink.udf.UDFJson';\n\ncreate source table topic1(\n    key varchar,\n    message varchar,\n    event_time bigint\n) with (\n    type = 'test'\n)\nWATERMARK event_time FOR rowtime BY ROWMAX_OFFSET(5000);  --event_time 为您的真实数据产生时间\n-- 定义 WATERMARK,通常您应该从kafka message中解析出event_time字段\n\n-- 定义数据流输出位置\ncreate sink table print_table_sink(\n    key varchar,\n    cnt long,\n    window_time varchar\n) with (\n    type = 'console',   -- print console\n    other = 'demo001'\n);\n\n\n-- 描述数据流计算过程\ninsert into print_table_sink\nwith tb2 as (\n    select key,\n    count(1),\n    cast(TUMBLE_START(rowtime,INTERVAL '5' SECOND) as varchar)|| '-->' \n    || cast(TUMBLE_END(rowtime,INTERVAL '5' SECOND) as varchar) AS window_time\n    from topic1 where key is not null\n    group by key,TUMBLE(rowtime,INTERVAL '5' SECOND)\n) select * from tb2","type":"StreamSql","config":"{\"taskManagerMemoryMb\":1024,\"taskManagerCount\":2,\"taskManagerSlots\":2,\"jobManagerMemoryMb\":1024,\"checkpointInterval\":-1,\"checkpointTimeout\":600000,\"checkpointDir\":\"hdfs:///tmp/sylph/flink/savepoints/\",\"minPauseBetweenCheckpoints\":0,\"parallelism\":4,\"queue\":\"default\",\"appTags\":[\"Sylph\",\"Flink\"],\"enableSavepoint\":false}","description":null,"jobId":9,"status":"STOP","runId":null,"appUrl":null}],
  'POST /api/login/account': async (req: Request, res: Response) => {
    const { password, username, type } = req.body;
    await waitTime(2000);
    if (password === 'admin' && username === 'admin') {
      res.send({
        status: 'ok',
        type,
        currentAuthority: 'admin',
      });
      access = 'admin';
      return;
    }
    if (password === 'admin' && username === 'user') {
      res.send({
        status: 'ok',
        type,
        currentAuthority: 'user',
      });
      access = 'user';
      return;
    }
    if (type === 'mobile') {
      res.send({
        status: 'ok',
        type,
        currentAuthority: 'admin',
      });
      access = 'admin';
      return;
    }

    res.send({
      status: 'error',
      type,
      currentAuthority: 'guest',
    });
    access = 'guest';
  },
  'GET /api/login/outLogin': (req: Request, res: Response) => {
    access = '';
    res.send({ data: {}, success: true });
  },
  'POST /api/register': (req: Request, res: Response) => {
    res.send({ status: 'ok', currentAuthority: 'user', success: true });
  },
  'GET /api/500': (req: Request, res: Response) => {
    res.status(500).send({
      timestamp: 1513932555104,
      status: 500,
      error: 'error',
      message: 'error',
      path: '/base/category/list',
    });
  },
  'GET /api/404': (req: Request, res: Response) => {
    res.status(404).send({
      timestamp: 1513932643431,
      status: 404,
      error: 'Not Found',
      message: 'No message available',
      path: '/base/category/list/2121212',
    });
  },
  'GET /api/403': (req: Request, res: Response) => {
    res.status(403).send({
      timestamp: 1513932555104,
      status: 403,
      error: 'Unauthorized',
      message: 'Unauthorized',
      path: '/base/category/list',
    });
  },
  'GET /api/401': (req: Request, res: Response) => {
    res.status(401).send({
      timestamp: 1513932555104,
      status: 401,
      error: 'Unauthorized',
      message: 'Unauthorized',
      path: '/base/category/list',
    });
  },

  'GET  /api/login/captcha': getFakeCaptcha,
};
