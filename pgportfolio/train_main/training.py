import logging
import os
import datetime

from pgportfolio.learn.tradertrainer import TraderTrainer
from pgportfolio.tools.configprocess import load_config

def train_main(device='cpu'):

    # logging level 설정
    console_level = logging.INFO
    logfile_level = logging.DEBUG

    # net_config.json 파일이 위치한 디렉토리
    config_dir = "train_package"

    # 디렉토리명을 현재 시간으로 설정 및 생성
    now_dt = datetime.datetime.today()
    now_str = now_dt.strftime('%y%m%d_%H%M')
    now_str = 'run' + now_str
    base_log_dir = config_dir + '/' + now_str
    os.makedirs(base_log_dir)

    # 프로그램 로그파일(programlog)와 콘솔 로그 레벨 설정
    # 파일에 출력할 로그와 콘솔에 출력할 로그를 구분한다는 의미임
    logging.basicConfig(filename=base_log_dir + '/programlog', level=logfile_level)
    console = logging.StreamHandler()
    console.setLevel(console_level)
    logging.getLogger().addHandler(console)

    print("training at {} started".format(now_str))
    tt =  TraderTrainer(load_config(), save_path=base_log_dir + '/netfile', device=device)
    train_retval = tt.train_net(log_file_dir=base_log_dir + '/tensorboard', index=now_str)
    print("training at {} finished".format(now_str))

    return train_retval
