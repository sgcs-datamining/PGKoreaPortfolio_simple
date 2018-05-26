from argparse import ArgumentParser
import pandas as pd
import pgportfolio.train_main.training
import pgportfolio.visualize.plot

# 명령형 인자 처리하는 함수
def args_parser():
    parser = ArgumentParser()
    parser.add_argument("--mode",dest="mode",
                        metavar="MODE", default="train")
    parser.add_argument("--device", dest="device", default="cpu",
                        help="device to be used to train")
    parser.add_argument("--plot", default=None,
                        help="테스트 결과를 그래프로 나타냄. 폴더 명을 쓰면 됨(e.g. run180418_2347)")
    return parser.parse_args()


def main():
    args = args_parser()

    if args.plot:
        pgportfolio.visualize.plot.plot_from_summary(args.plot)
    elif args.mode == "train":
        pgportfolio.train_main.training.train_main(args.device)


if __name__ == "__main__":
    main()
