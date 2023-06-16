import builtins
import sys
import transformations.transform as t


def main():
    print(t.sum(1, 2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
