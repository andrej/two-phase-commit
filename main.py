import argparse
import sys
import node


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--coordinator", action="store_true")
    argparser.add_argument("--port", type=int)
    argparser.add_argument("--hostname", type=str, default="localhost")
    args = argparser.parse_args()
    if args.coordinator:
        the_node = node.TwoPhaseCommitCoordinator()
    else:
        the_node = node.TwoPhaseCommitParticipant()


if __name__ == "__main__":
    main()
