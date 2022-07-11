package com.spdb.hive.sync.kill;

import com.spdb.hive.sync.constant.Constant;
import com.spdb.hive.sync.entity.MainTask;
import com.spdb.hive.sync.service.MainTaskService;
import com.spdb.hive.sync.util.date.DateUtil;
import com.spdb.hive.sync.util.parser.OptionParserUtil;
import com.spdb.hive.sync.util.property.PropUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * project：hive-sync-1.2
 * package：com.spdb.hive.sync.kill
 * author：zhouxh
 * time：2021-07-01 9:20
 * description：手动kill正在运行的数据同步进程
 */
public class Kill {
    // Logger和LoggerFactory导入的是org.slf4j包
    private final static Logger logger = LogManager.getLogger(Kill.class);
    private static MainTaskService mainTaskService = new MainTaskService();

    public static void main(String[] args) {
        if (args != null && args.length == 2) {
            //  参数解析
            int flg = OptionParserUtil.optionParse(args);
            if (flg < 0) return;
            //  数据同步
            String killId = PropUtil.getProValue(Constant.TASK_STATUS_KILL);
            if (killId != null) {
                int mainTaskId = Integer.valueOf(killId);
                kill(mainTaskId);
            }
        } else {
            logger.error("参数解析错误，-kill 参数后请传递正确的主任务ID");
        }
    }

    public static void kill(int mainTaskId) {
        //  查看当前主任务运行状态和结果
        MainTask mainTask = mainTaskService.queryById(mainTaskId);
        if (mainTask == null) {
            logger.error("当前传入的主任务ID无法查询到相关记录，请传入有效的主任务ID");
            return;
        }
        if (Constant.TASK_STATUS_FINISH.equalsIgnoreCase(mainTask.getMainTaskStatus())) {
            logger.info("当前主任务已经运行结束，无需手动kill");
        } else {
            //  更新main_task_status字段为finish
            mainTask.setMainTaskStatus(Constant.TASK_STATUS_FINISH);
            //  更新main_task_result字段为kill
            mainTask.setMainTaskResult(Constant.TASK_STATUS_KILL);
            mainTask.setStopTime(DateUtil.getDate());
            int flag = mainTaskService.update(mainTask);
            if (flag > 0) {
                logger.info("当前主任务已经kill，正在处理的数据表处理完成后进程退出，未处理的处理表不再处理");
            } else {
                logger.error("当前主任务kill失败，请查看报错日志");
            }
        }
    }
}
