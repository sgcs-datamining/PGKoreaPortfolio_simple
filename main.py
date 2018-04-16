from argparse import ArgumentParser

import pgportfolio.train_main.training

# 명령형 인자 처리하는 함수
def args_parser():
    parser = ArgumentParser()
    parser.add_argument("--mode",dest="mode",
                        metavar="MODE", default="train")
    parser.add_argument("--device", dest="device", default="cpu",
                        help="device to be used to train")
    return parser.parse_args()


def main():
    args = args_parser()

    if args.mode == "train":
        pgportfolio.train_main.training.train_main(args.device)


if __name__ == "__main__":
    main()
