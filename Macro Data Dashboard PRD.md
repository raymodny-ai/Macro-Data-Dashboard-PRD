# **宏观流动性与资产定价状态识别系统：全栈工程产品需求文档**

## **一、 业务愿景与宏观范式重构背景**

在当前的全球宏观金融生态中，底层资产的定价逻辑正在经历一次历史性的重构。根据特定前瞻性推演所揭示的逻辑架构，美联储的政策行为模式将彻底告别长久以来的“前瞻指引（Forward Guidance）”与“美联储看跌期权（Fed Put）”双重福利时代。新一任决策层（如假定的沃什路线）不再简单地在“鹰派”或“鸽派”之间摇摆，而是转向了一种冷酷的“清算主义（Liquidationism）”。在这一范式下，美联储为避免因宏观数据的反复而积累“声誉债务（Reputational Debt）”，将采取“少承诺、看数据、短声明”的全新政策沟通模式。  
这种彻底撕毁利率路线图和免费兜底保单的行为，意味着美联储将不再充当免费消化市场波动的隐形卖方。未来的基准市场资产定价走势将呈现极具杀伤力的“熊陡（Bear Steepener）”状态，即短端利率因坚挺的核心通胀与工资动量被死死钉在高位，而长端利率则因巨额的联邦财政赤字（例如2026财年高达2.06万亿美元的白宫预算预期）以及科技巨头的海量人工智能资本开支，面临期限溢价狂飙的压力。市场生态将从被动等待央行剧透的温室，转变为低承诺、看数据、高波动的丛林。  
为了在这一极端环境中生存并获利，机构投资者必须完成一次根本性的心智切换，从被动的预期交易者转型为冷酷的“状态识别者（State-Identifier）”。基于这一核心诉求，本产品需求文档（PRD）定义并设计了一套全栈工程架构——“状态识别者”宏观监控终端。该系统的核心使命是每日自动化爬取、清洗、融合并计算全市场最难被操纵的五组硬核物理数据。通过穿透政策噪音，系统将严格划分市场的“健康疼痛区（左线正常资产重新定价）”与“功能瘫痪区（右线流动性机制崩溃）”，从而在美联储被迫动用流动性工具箱（第二杠杆）时，准确判定其究竟是战术性的水管修补，还是战略上向“财政主导（Fiscal Dominance）”彻底投降的底线失守。

## **二、 系统总体架构设计与技术栈选型**

为承载高频宏观数据的汇聚、长周期的时序计算以及复杂的跨资产联动分析，系统的底层架构必须兼顾海量吞吐能力、复杂关系查询以及极低的数据延迟。整体架构被严密划分为数据采集层、底层存储与时序计算引擎、业务逻辑判定矩阵以及前端可视化大屏四个核心模块。

### **存储引擎选型：时序数据库的架构权衡**

在底层时序数据库（Time-Series Database, TSDB）的选择上，工程团队对ClickHouse与TimescaleDB进行了深度的性能与适用性评估。虽然ClickHouse在十亿行级别的纯分析型（OLAP）查询中具有显著的查询速度优势与极致的压缩率（例如针对日志或高频交易Tick数据）1，但本系统的核心痛点并非单纯的超大规模日志吞吐。本系统需要处理的是跨度达数十年的宏观面板数据、每日千万级以下的更新频次，且高度依赖复杂的关系型联表查询（JOIN），例如将SEC的财务元数据与每日的流动性利差及国债拍卖结果进行深度关联。  
基于这些硬性需求，系统最终采用TimescaleDB作为核心存储引擎。TimescaleDB作为PostgreSQL的扩展模块，其“超表（Hypertables）”机制能够实现按时间维度的自动分区，在保持完全ACID事务合规性的同时，提供了卓越的连续聚合（Continuous Aggregates）功能1。在处理针对特定时间窗口与服务维度的过滤查询时（例如计算SOFR与IORB利差的历史分位数），TimescaleDB的查询延迟在百万级数据规模下依然能保持在极低的亚毫秒级别4。此外，它不仅支持原生的PostgreSQL工具链和SQL语法，还能以原生方式处理复杂的金融级事务状态流转，完美契合“状态识别者”模型对于准确性与数据完整性的严苛要求5。

### **异步任务编排与数据管道**

系统的数据采集层依赖于Apache Airflow进行有向无环图（DAG）的任务编排。由于所依赖的外部数据源（如FRED、SEC EDGAR、US Treasury Fiscal Data、Alpha Vantage等）更新频率不一，存在日频、周频、月频乃至季度频的差异，Airflow的动态调度机制能够确保各项数据在发布后的第一时间被并行拉取。针对各大API的速率限制（Rate Limits），系统在中间件层集成了基于Redis的令牌桶（Token Bucket）限流器，并配合指数退避（Exponential Backoff）重试算法及死信队列（Dead Letter Queue），以保障在应对如SEC EDGAR等严格限流接口时的绝对高可用性与数据无损落地。

## **三、 数据引擎模块一：通胀二阶导组 (Inflation Second-Derivative Group)**

在清算主义框架下，美联储的“第一根杠杆”即宏观利率立场（Interest Rate Stance）主要锚定于物价的底层薪柴燃烧状态。只要核心通胀与劳动力工资动量未出现断崖式坍塌，短端利率的下限将极其坚硬。通胀二阶导组的核心任务，就是从平缓的月度数据中提取出反映价格变动加速度的高阶信号。

### **数据源与指标定义**

系统直接对接圣路易斯联储经济数据（FRED）API，这组数据虽然不属于高频指标（每月发布一次），但其物理真实性极高。 首先，系统追踪核心通胀的环比变化趋势。系统将调用“Consumer Price Index for All Urban Consumers: All Items Less Food and Energy”（简称核心CPI，Series ID: CPILFESL）7。该指标剔除了波动剧烈的食品与能源价格，真实反映了经济体内部的服务与核心商品价格粘性7。同时，系统为提供交叉验证，还会同步抓取核心商品价格指数（Commodities Less Food and Energy Commodities）等子项指标10。 其次，为了监控劳动力市场的薪资螺旋，系统调用BLS发布的“Average Hourly Earnings of All Employees, Total Private”（全美私人部门平均时薪，Series ID: CES0500000003）12。为进一步细分，系统还将提取制造业（Series ID: CES3000000008）、休闲酒店业（Series ID: CES7000000003）及信息产业（Series ID: CES5000000003）的细分时薪数据，以评估工资动量在不同行业的横向扩散程度14。

### **数据处理与数学模型**

在ETL清洗入库后，系统的聚合计算层会立即执行导数运算。对于核心CPI与时薪数据，模型不仅计算传统的一阶环比增速（MoM，即当期数值除以前期数值减一），更关键的是提取“二阶动量加速度”。 算法上，系统首先对过去三个月的环比增速应用移动平均（3MMA）进行平滑处理，随后计算当前3MMA与前置3MMA的差值。当二阶导数连续两个月呈现正值并伴随斜率扩大时，系统将发出“薪柴复燃”的预警信号，明确提示短端利率的宽松窗口已被彻底封死。

### **数据库超表结构设计**

| 字段名称 (Column) | 数据类型 (Type) | 约束与说明 (Description) |
| :---- | :---- | :---- |
| record\_date | DATE | 报告期时间，主键（Hypertable时间分区键） |
| core\_cpi\_index | NUMERIC(10,4) | CPILFESL 原始核心CPI数值7 |
| core\_cpi\_mom | NUMERIC(8,4) | 核心CPI环比增长率 (%) |
| core\_cpi\_accel | NUMERIC(8,4) | 核心CPI二阶加速度 |
| wage\_hourly\_abs | NUMERIC(8,2) | CES0500000003 绝对每小时工资 ($)12 |
| wage\_mom | NUMERIC(8,4) | 工资环比增长率 (%) |
| wage\_accel | NUMERIC(8,4) | 工资二阶加速度 |

## **四、 数据引擎模块二：财政增量组 (Fiscal Supply Group)**

“熊陡”基准形态的另一端，是长端利率的疯狂上翘。这背后的物理推手是白宫巨额的财政赤字预期（如2025及2026财年逼近2万亿美元的规模），迫使美国财政部向市场天量倾泻长久期国债。财政增量组旨在量化这一供给冲击对长端利率承压情况以及市场自发吸收能力的破坏程度。

### **数据源与指标定义**

第一个关键指标是国债拍卖的认购倍数（Bid-to-Cover Ratio）。该指标等于收到的投标总金额除以最终接受的投标总金额（即计划发行的总金额），高认购倍数代表需求强劲，而异常走低则意味着市场承接力枯竭18。系统通过集成US Treasury Fiscal Data API，针对端点 /v1/accounting/od/upcoming\_auctions 与 /v1/accounting/od/treasury-securities-auctions 抓取每日的国债拍卖数据20。系统特别筛选期限为10年期（10-Year）及30年期（30-Year）的长端国债数据22。此外，系统还会关注拍卖中的“尾部（Tail）”现象（即最高接受收益率与发行前市场预期收益率之间的差值）以及一级交易商（Primary Dealers）的被迫接盘比例，以此深度剖析需求的微观结构19。  
第二个关键指标是期限溢价（Term Premium）。投资者为承担长久期国债带来的利率与通胀风险，会要求更高的额外补偿。系统通过FRED API拉取纽约联储构建的ACM期限溢价模型数据（Adrian, Crump, and Moench Model）26。具体而言，系统每日提取“Term Premium on a 10 Year Zero Coupon Bond”（Series ID: THREEFYTP10）29。该模型使用五因子动态无套利仿射期限结构模型，剔除了市场对未来短期利率路径的单纯预期，精准剥离出纯粹的风险溢价部分27。

### **物理推演与预警逻辑**

该模块将认购倍数的移动平均值与ACM 10年期期限溢价的走势进行拟合计算。如果在巨量长端国债发行周，系统监测到认购倍数跌破历史中位数（例如连续跌破2.4），同时ACM期限溢价突破前高（例如升至0.75%以上）19，说明市场吸收巨额赤字的自发能力正在解体。此时，收益率曲线的长端将被无情抬升，系统将在前端大屏直接绘制并锁定“熊陡”形态警告。

### **数据库超表结构设计**

| 字段名称 (Column) | 数据类型 (Type) | 约束与说明 (Description) |
| :---- | :---- | :---- |
| auction\_date | DATE | 拍卖执行日期22 |
| security\_term | VARCHAR(20) | 证券期限（筛选 10-Year, 30-Year）22 |
| bid\_to\_cover | NUMERIC(6,3) | 认购倍数（Bid-to-Cover Ratio）33 |
| auction\_tail | NUMERIC(6,4) | 拍卖尾部点差（Tail Spread） |
| acm\_tp\_10y | NUMERIC(6,4) | THREEFYTP10 10年期ACM期限溢价 (%)29 |

## **五、 数据引擎模块三：流动性走廊组 (Liquidity Corridor Group)**

本组数据是“状态识别者”模型中最核心、最致命的枢纽。在隔夜逆回购（RRP）水塔完全枯竭的残酷现实下，它直接决定了金融系统当前处于左线的“健康疼痛区（估值回调）”还是右线的“功能瘫痪区（流动性断裂）”。它帮助投资者剥离美联储的“宏观利率立场”与“流动性修补工具”双轨杠杆。

### **数据源与利差构建**

系统依靠FRED API高频拉取短期流动性市场的定价核心数据。第一个指标是担保隔夜融资利率（Secured Overnight Financing Rate, SOFR，Series ID: SOFR），它是基于美国国债抵押的隔夜借贷成本的广义衡量标准，也是目前最重要的市场流动性指示器34。第二个指标是准备金余额利率（Interest Rate on Reserve Balances, IORB，Series ID: IORB），即美联储为存款机构存放在央行的准备金支付的利率36。

### **触发器线与“水管爆裂”判定**

系统的ETL管道在内存中对每日发布的SOFR与IORB数值进行对齐，并实时计算它们之间的利差（Spread \= SOFR \- IORB）36。 在流动性充裕的常态下，SOFR往往低于IORB，利差处于负值区间（例如维持在-0.05个百分点左右）。然而，一旦银行间准备金真实陷入短缺，机构为获取现金将不惜高价拆借，SOFR会迅速飙升并向IORB逼近。 系统引擎内置了严苛的三级状态触发器：

* **状态0（绿灯，左线）**：利差长期稳定在负值。任何此时股市或债市的大幅下挫，均被系统无情判定为“市场给自己的风险重新定价”。美联储绝对不会提供免费保单，投资者切忌盲目抄底。  
* **状态1（黄灯，临界压迫）**：利差开始收敛并逼近零轴。此时底层“水管”已经嘎嘎作响。  
* **状态2（红灯，右线功能瘫痪）**：利差转正（SOFR \> IORB）并发生脉冲式走阔。此时系统报警基础设施崩溃。在这种极端物理状态下，美联储的第二杠杆（如常备回购便利SRF等流动性工具）将被迫拧开。

若系统进一步监测到紧急水龙头不仅开启，且释放规模随着财政赤字的扩张而呈指数级飙升且无法收回，状态识别器将输出最高级别的战略警告——系统已彻底滑入“财政主导（Fiscal Dominance）”，美元信用基石受损，所有的传统资产配置逻辑均需即刻调整。

### **数据库超表结构设计**

| 字段名称 (Column) | 数据类型 (Type) | 约束与说明 (Description) |
| :---- | :---- | :---- |
| record\_date | DATE | 交易日期 |
| sofr\_rate | NUMERIC(6,3) | SOFR 担保隔夜融资利率 (%)35 |
| iorb\_rate | NUMERIC(6,3) | IORB 准备金余额利率 (%)37 |
| sofr\_iorb\_spread | NUMERIC(6,4) | 核心利差 (SOFR 减 IORB)36 |
| system\_state | INTEGER | 流动性状态判定码 (0: 充裕, 1: 紧张, 2: 瘫痪) |

## **六、 数据引擎模块四：AI资本开支与瓶颈组 (AI CapEx & Bottleneck Group)**

未来的长期宏观中性利率（Neutral Rate）中枢不仅受制于政府的财政赤字，正以前所未有的程度受到科技巨头在AI基建（服务器、数据中心、电力变压器与铜需求）上的疯狂资本开支（CapEx）的向上推举。这使得实体经济在有限的资金池中展开激烈的抽水竞争。本组数据负责跟踪科技寡头的发债需求与实物投资规模，评估中性利率向外推移的斜率。

### **数据源与API集成逻辑**

为获取最权威的企业资本开支数据，系统采用了双冗余架构。 主要通道直接对接美国证券交易委员会（SEC）的EDGAR RESTful API。通过调用 /api/xbrl/companyconcept/ 端点，系统定向爬取顶级科技公司（通过CIK码精确定位，如Apple的CIK为32019339）在10-Q及10-K财报中的XBRL结构化标签41。核心提取的会计标签包括代表实物资本购建的 us-gaap/PaymentsToAcquirePropertyPlantAndEquipment 以及代表研发护城河的 us-gaap/ResearchAndDevelopmentExpense。 在此处，系统必须克服SEC接口严苛的公平访问规则（Fair Access Policy），即请求频率严格限制在每秒10次以内，且必须附带合规的 User-Agent39。工程上，我们部署了基于Celery的异步任务队列，实施精确的毫秒级间隔限流与重试机制。 备用通道则利用Alpha Vantage的Fundamental Data API（端点为 CASH\_FLOW 与 INCOME\_STATEMENT）43，在财报季进行批量比对与校验验证，通过提取其 capitalExpenditures 和 researchAndDevelopment 字段确保数据的完整性44。

### **动量指数构建与经济影响推演**

系统将七大至十大科技巨头的季度CapEx绝对额进行加权汇总，并计算其环比及同比增速。当系统发现该“AI CapEx动量指数”以年化数千亿美元的规模持续陡增时，结合市场上的信用债发行总量，计算引擎将据此修正远期无风险利率的基线模型。这种实体端的抢钱行为，本质上使得美联储现有的名义利率水平显得不再具有足够的“限制性”，进而从微观层面证伪了美联储能够轻易开启降息周期的幻想。

### **数据库超表结构设计**

| 字段名称 (Column) | 数据类型 (Type) | 约束与说明 (Description) |
| :---- | :---- | :---- |
| filing\_date | DATE | 财报提交或披露日期41 |
| cik | VARCHAR(10) | SEC中央索引键 (CIK)39 |
| ticker | VARCHAR(10) | 证券代码 |
| capex\_amount | BIGINT | 提取的资本支出绝对额 (USD) |
| rd\_expense | BIGINT | 研发支出绝对额 (USD)44 |
| ai\_capex\_index | NUMERIC(10,4) | 聚合计算的 AI CapEx 动量指数 |

## **七、 数据引擎模块五：市场传染组 (Market Contagion Group)**

在彻底失去了美联储的“免费看跌保单”后，金融系统随时可能发生风险跨资产的恶性传染。投资者需要一个宏观维度的“地震仪”，以判定单资产的“健康疼痛”是否正在演变为引发系统性雪崩的“扭曲平整”甚至全面紧缩的“熊平（Bear Flattener）”踩踏。

### **数据源与跨资产监控指标**

系统首先高度关注美国国债市场的恐慌情绪，其核心观测变量为ICE BofAML MOVE Index（常被称为债市的VIX）。系统通过集成Yahoo Finance API（使用Python的 yfinance 模块或相关行情接口），抓取 ^MOVE 或其3个月期变体 ^MOVE3M 的历史与实时日线数据45。相较于只反映股市波动的VIX，MOVE指数直接衡量固定收益市场的隐含波动率。一旦MOVE指数发生急剧的无序飙升（如突破历史均值达到120甚至更高），即标志着国债作为底层抵押品的价格剧烈动荡，这往往是信用机制滑向瘫痪的前兆。  
其次，系统通过Alpha Vantage的核心股票数据接口（TIME\_SERIES\_DAILY\_ADJUSTED）提取标普500 ETF（如SPY）与20年期以上长期国债ETF（如TLT）的每日复权收盘价（Adjusted Close）43。

### **相关性矩阵与传染诊断模型**

传统的经典投资组合理论（如60/40策略）建立在股债呈现负相关性的基础之上（股市下跌时，国债价格上涨充当避风港）。然而，在清算主义与财政主导的混合双击下，由于长端债券本身即是巨大的风险源头，股债往往会发生残酷的双杀。 系统每日基于前置清洗的对数收益率数据，计算长短期（如30日、60日）的滚动皮尔逊相关系数（Rolling Pearson Correlation）。 当判定矩阵同时满足以下条件时，系统会触发最高级别的“市场传染警报”：

1. 股票市场大幅下挫；  
2. 股债相关系数由负转为极端的正向收敛（即股跌债也暴跌）；  
3. MOVE指数脱离正常运行区间发生脉冲式跳升45。 这套逻辑将彻底打消投资者面对美股暴跌时“抄底等待美联储降息”的陈旧思维。

### **数据库超表结构设计**

| 字段名称 (Column) | 数据类型 (Type) | 约束与说明 (Description) |
| :---- | :---- | :---- |
| trade\_date | DATE | 交易日期 |
| move\_index | NUMERIC(8,4) | ICE BofAML MOVE 指数日收盘值45 |
| spy\_log\_return | NUMERIC(8,4) | SPY日对数收益率 |
| tlt\_log\_return | NUMERIC(8,4) | TLT日对数收益率 |
| rolling\_corr\_30d | NUMERIC(6,4) | 30日滚动跨资产相关系数 |
| contagion\_alert | BOOLEAN | 综合传染状态警报（True/False） |

## **八、 状态识别引擎与独立双轨控制模型大屏**

所有底层抓取并计算的五大数据组，最终都将汇入系统的前端状态识别引擎，构建出直观且冷酷的“独立双轨控制模型（Independent Dual-Track Control Mindset）”大屏。  
该前端面板采用响应式的暗色调（Dark Mode）设计，使用基于WebGL的图表库（如Three.js结合Apache ECharts）渲染复杂的三维图形。 在大屏的核心区域，系统将实时绘制美国国债收益率的3D动态曲面（3D Yield Curve Surface）。横轴为到期期限（从隔夜到30年），纵轴为收益率绝对值，时间轴则展示曲线在过去数周的扭曲演化。结合前述的通胀二阶导组与财政增量组数据，算法将自动为当前的曲面形态打上标签，如果长端斜率陡增而短端死锁，屏幕将鲜明提示当前的基准态势为“熊陡（Bear Steepener）”。系统同时监控“熊平（Bear Flattener，因通胀粘性迫使短端暴力拉升压平曲线）”以及局部供给失衡引发的“扭曲平整（Twist Flattener）”异常路径。  
在面板的两侧，布置着独立双轨状态矩阵图： 第一轨（宏观立场轨道）将通胀粘性与AI资本开支指标聚合为一个“宏观紧缩指数（Macro Restrictive Index）”，明确显示大环境是否仍需高息压制； 第二轨（流动性修补轨道）则严密挂钩SOFR-IORB利差与MOVE指数构成的“功能瘫痪仪表盘”。  
如果市场仅发生左线疼痛，而新闻却爆出美联储动用了某项定向回购工具，系统会通过这套双轨矩阵进行实时解码，在屏幕上提示：此操作仅归属第二轨“局部水管修补”，而第一轨宏观立场未变。因此，系统会强制警告交易员：这绝不是转向宽松的降息信号，严禁盲目全仓做多风险资产。只有当就业数据出现真正意义上的断崖式跨塌，且通胀与工资动量同步大幅掉落时，大屏上降息逻辑回归的绿色指示灯才会真正亮起。

## **九、 结论**

在这场由于宏观游戏规则突变引发的定价逻辑颠覆中，“等待央行发牌”的被动时代已宣告终结。本篇全栈工程产品需求文档通过深度的系统架构规划，将深奥抽象的“清算主义”哲学与双杠杆理论，解构为了可编程、可爬取、可计算的五组硬核物理指标集。  
通过结合TimescaleDB的高效时序存储、分布式的数据管道编排，以及严密的多重判定算法，“状态识别者”平台不仅能够精准刻画“熊陡”基准下的通胀薪柴与财政承压真相，更能以前所未有的敏锐度，在流动性利差（SOFR-IORB）转正的微观瞬间，捕获系统滑向功能瘫痪与财政主导的右线致命深渊。该系统不仅是机构投资者规避新型宏观尾部风险的终极防御装甲，更是其在这个低承诺、高波动的新丛林生态中，主动出击捕获结构性重定价红利的核心武器。  
*This is for informational purposes only. For medical advice or diagnosis, consult a professional.*

#### **引用的著作**

1. ClickHouse vs TimescaleDB 2026: Time-Series Database Comparison \- Tasrie IT Services, [https://tasrieit.com/blog/clickhouse-vs-timescaledb-2026](https://tasrieit.com/blog/clickhouse-vs-timescaledb-2026)  
2. ClickHouse vs TimescaleDB vs InfluxDB: Which Time-Series Database for Your Analytics?, [https://blog.elest.io/clickhouse-vs-timescaledb-vs-influxdb-which-time-series-database-for-your-analytics/](https://blog.elest.io/clickhouse-vs-timescaledb-vs-influxdb-which-time-series-database-for-your-analytics/)  
3. ClickHouse vs TimescaleDB: Which to Choose for Time-Series Data \- OneUptime, [https://oneuptime.com/blog/post/2026-01-21-clickhouse-vs-timescaledb/view](https://oneuptime.com/blog/post/2026-01-21-clickhouse-vs-timescaledb/view)  
4. I Benchmarked TimescaleDB vs ClickHouse vs MongoDB for Observability Data \- The Results Surprised Me \- DEV Community, [https://dev.to/aws-builders/i-benchmarked-timescaledb-vs-clickhouse-vs-mongodb-for-observability-data-the-results-surprised-me-3d7d](https://dev.to/aws-builders/i-benchmarked-timescaledb-vs-clickhouse-vs-mongodb-for-observability-data-the-results-surprised-me-3d7d)  
5. ClickHouse vs TimescaleDB. What is the difference? A detailed comparison | by Data Engineer | DoubleCloud | Medium, [https://medium.com/doublecloud-insights/clickhouse-vs-timescaledb-what-is-the-difference-a-detailed-comparison-62127a989d8d](https://medium.com/doublecloud-insights/clickhouse-vs-timescaledb-what-is-the-difference-a-detailed-comparison-62127a989d8d)  
6. PostgreSQL vs. TimescaleDB vs. ClickHouse: 2026 Performance Guide \- sanj.dev, [https://sanj.dev/post/postgresql-timescaledb-clickhouse-comparison/](https://sanj.dev/post/postgresql-timescaledb-clickhouse-comparison/)  
7. Consumer Price Index for All Urban Consumers: All Items Less Food and Energy in U.S. City Average (CPILFESL) | FRED, [https://fred.stlouisfed.org/series/CPILFESL](https://fred.stlouisfed.org/series/CPILFESL)  
8. CPI, Core, Monthly \- Economic Data Series | FRED | St. Louis Fed, [https://fred.stlouisfed.org/tags/series?t=core%3Bcpi%3Bmonthly](https://fred.stlouisfed.org/tags/series?t=core;cpi;monthly)  
9. US Core Consumer Price Index MoM (Monthly) \- United States … \- YCharts, [https://ycharts.com/indicators/us\_core\_consumer\_price\_index\_mom](https://ycharts.com/indicators/us_core_consumer_price_index_mom)  
10. Commodities, CPI, Core \- Economic Data Series | FRED | St. Louis Fed, [https://fred.stlouisfed.org/tags/series?t=commodities%3Bcore%3Bcpi](https://fred.stlouisfed.org/tags/series?t=commodities;core;cpi)  
11. CPI, Core \- Economic Data Series | FRED | St. Louis Fed, [https://fred.stlouisfed.org/tags/series?t=core%3Bcpi](https://fred.stlouisfed.org/tags/series?t=core;cpi)  
12. Average Hourly Earnings of All Employees, Total Private (CES0500000003) \- FRED, [https://fred.stlouisfed.org/series/CES0500000003](https://fred.stlouisfed.org/series/CES0500000003)  
13. Average Hourly Earnings of All Employees, Total Private (CEU0500000003) \- FRED, [https://fred.stlouisfed.org/series/CEU0500000003](https://fred.stlouisfed.org/series/CEU0500000003)  
14. Average Hourly Earnings of Production and Nonsupervisory Employees, Manufacturing (CES3000000008) | FRED | St. Louis Fed, [https://fred.stlouisfed.org/series/CES3000000008](https://fred.stlouisfed.org/series/CES3000000008)  
15. Average Hourly Earnings of All Employees, Leisure and Hospitality (CES7000000003) | FRED | St. Louis Fed, [https://fred.stlouisfed.org/series/CES7000000003](https://fred.stlouisfed.org/series/CES7000000003)  
16. Average Hourly Earnings of All Employees, Professional and Business Services | FRED, [https://fred.stlouisfed.org/graph/?g=1IU5x](https://fred.stlouisfed.org/graph/?g=1IU5x)  
17. Average Hourly Earnings of All Employees, Information (CES5000000003) \- FRED, [https://fred.stlouisfed.org/series/CES5000000003](https://fred.stlouisfed.org/series/CES5000000003)  
18. US \- 10Y Treasury Bid-to-Cover Ratio, [https://en.macromicro.me/collections/51/us-treasury-bond/30431/us-10y-bid-to-cover-ratio](https://en.macromicro.me/collections/51/us-treasury-bond/30431/us-10y-bid-to-cover-ratio)  
19. How to tell if the US Treasury is having trouble borrowing in the bond market | Brookings, [https://www.brookings.edu/articles/how-to-tell-if-the-us-treasury-is-having-trouble-borrowing-in-the-bond-market/](https://www.brookings.edu/articles/how-to-tell-if-the-us-treasury-is-having-trouble-borrowing-in-the-bond-market/)  
20. Treasury Securities Upcoming Auctions Data, [https://fiscaldata.treasury.gov/datasets/upcoming-auctions/](https://fiscaldata.treasury.gov/datasets/upcoming-auctions/)  
21. API Documentation \- U.S. Treasury Fiscal Data, [https://fiscaldata.treasury.gov/api-documentation/](https://fiscaldata.treasury.gov/api-documentation/)  
22. Treasury Securities Auctions Data, [https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/](https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/)  
23. Fed Treasury Python API Docs | dltHub, [https://dlthub.com/context/source/fed-treasury](https://dlthub.com/context/source/fed-treasury)  
24. Announcements, Data & Results \- TreasuryDirect, [https://www.treasurydirect.gov/auctions/announcements-data-results/](https://www.treasurydirect.gov/auctions/announcements-data-results/)  
25. Understanding US Treasury Auctions What You Need to Know \- Saxo Bank, [https://www.home.saxo/content/articles/bonds/understanding-us-treasury-auctions-what-you-need-to-know-20082024](https://www.home.saxo/content/articles/bonds/understanding-us-treasury-auctions-what-you-need-to-know-20082024)  
26. ACM term premium Archives \- Liberty Street Economics, [https://libertystreeteconomics.newyorkfed.org/tag/acm-term-premium/](https://libertystreeteconomics.newyorkfed.org/tag/acm-term-premium/)  
27. The Fed \- Robustness of long-maturity term premium estimates \- Federal Reserve, [https://www.federalreserve.gov/econres/notes/feds-notes/robustness-of-long-maturity-term-premium-estimates-20170403.html](https://www.federalreserve.gov/econres/notes/feds-notes/robustness-of-long-maturity-term-premium-estimates-20170403.html)  
28. Treasury Term Premia: 1961-Present \- Liberty Street Economics \- Federal Reserve Bank of New York, [https://libertystreeteconomics.newyorkfed.org/2014/05/treasury-term-premia-1961-present/](https://libertystreeteconomics.newyorkfed.org/2014/05/treasury-term-premia-1961-present/)  
29. Term Premium on a 10 Year Zero Coupon Bond (THREEFYTP10) | FRED | St. Louis Fed, [https://fred.stlouisfed.org/series/THREEFYTP10](https://fred.stlouisfed.org/series/THREEFYTP10)  
30. Term Premium \- Economic Data Series | FRED | St. Louis Fed, [https://fred.stlouisfed.org/tags/series?t=term+premium](https://fred.stlouisfed.org/tags/series?t=term+premium)  
31. US \- ACM 10Y Treasury Term Premium Estimates \- MacroMicro, [https://en.macromicro.me/charts/45452/us-10-treasury-term-premium](https://en.macromicro.me/charts/45452/us-10-treasury-term-premium)  
32. US Treasury auctions hold firm amid fiscal and policy jitters \- CEIC, [https://info.ceicdata.com/us-treasury-auctions-hold-firm-amid-fiscal-and-policy-jitters](https://info.ceicdata.com/us-treasury-auctions-hold-firm-amid-fiscal-and-policy-jitters)  
33. Record-Setting Treasury Securities Auction Data, [https://fiscaldata.treasury.gov/datasets/record-setting-auction-data/](https://fiscaldata.treasury.gov/datasets/record-setting-auction-data/)  
34. Secured Overnight Financing Rate (SOFR) | FRED | St. Louis Fed, [https://fred.stlouisfed.org/series/SOFR](https://fred.stlouisfed.org/series/SOFR)  
35. Secured Overnight Financing Rate Data \- FEDERAL RESERVE BANK of NEW YORK, [https://www.newyorkfed.org/markets/reference-rates/sofr](https://www.newyorkfed.org/markets/reference-rates/sofr)  
36. US \- SOFR Minus IORB Spread \- MacroMicro, [https://en.macromicro.me/charts/141325/us-sofriorb-spread](https://en.macromicro.me/charts/141325/us-sofriorb-spread)  
37. Secured Overnight Financing Rate | FRED | St. Louis Fed, [https://fred.stlouisfed.org/graph/?g=13b5S](https://fred.stlouisfed.org/graph/?g=13b5S)  
38. Interest Rate on Reserve Balances (IORB Rate) | FRED | St. Louis Fed, [https://fred.stlouisfed.org/graph/?id=IORB,SOFR,EFFR,](https://fred.stlouisfed.org/graph/?id=IORB,SOFR,EFFR,)  
39. Extracting data from SEC EDGAR RESTful APIs \- Kaggle, [https://www.kaggle.com/code/svendaj/extracting-data-from-sec-edgar-restful-apis](https://www.kaggle.com/code/svendaj/extracting-data-from-sec-edgar-restful-apis)  
40. sec-edgar-api \- Read the Docs, [https://sec-edgar-api.readthedocs.io/](https://sec-edgar-api.readthedocs.io/)  
41. SEC EDGAR Filings API, [https://sec-api.io/](https://sec-api.io/)  
42. EDGAR Application Programming Interfaces (APIs) \- SEC.gov, [https://www.sec.gov/search-filings/edgar-application-programming-interfaces](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)  
43. Alpha Vantage API Documentation, [https://www.alphavantage.co/documentation/](https://www.alphavantage.co/documentation/)  
44. Corporate Financials Statements (AlphaVantage) \- Kaggle, [https://www.kaggle.com/datasets/emranalbiek/companies-financial-income-statements](https://www.kaggle.com/datasets/emranalbiek/companies-financial-income-statements)  
45. ICE BofAML MOVE Index (^MOVE) Historical Data \- Yahoo\! Finance Canada, [https://ca.finance.yahoo.com/quote/%5EMOVE/history/](https://ca.finance.yahoo.com/quote/%5EMOVE/history/)  
46. ICE BofAML MOVE 3-Month Index (^MOVE3M) options chain \- Yahoo Finance, [https://sg.finance.yahoo.com/quote/%5EMOVE3M/options/](https://sg.finance.yahoo.com/quote/%5EMOVE3M/options/)  
47. ICE BofAML MOVE 3-Month Index (^MOVE3M) options chain \- Yahoo Finance, [https://nz.finance.yahoo.com/quote/%5EMOVE3M/options/](https://nz.finance.yahoo.com/quote/%5EMOVE3M/options/)  
48. Alpha Vantage MCP Server, [https://mcp.alphavantage.co/](https://mcp.alphavantage.co/)  
49. Candlestick Subplots with Plotly and the AlphaVantage API \- QuantStart, [https://www.quantstart.com/articles/candlestick-subplots-with-plotly-and-the-alphavantage-api/](https://www.quantstart.com/articles/candlestick-subplots-with-plotly-and-the-alphavantage-api/)