import sys


class Car:
    def __init__(self, key="000", max_kmh=180):
        self.__key = key
        self.max_kmh = max_kmh

    def get_key(self):
        return self.__key

    def show_information(self):
        print("Llave: " + self.__key + "; vel. max: " + str(self.max_kmh))


def main():
    car1 = Car("123")
    car2 = Car("456")

    print(car1.get_key())
    print(car2.get_key())
    car1.show_information()


if __name__ == "__main__":
    main()
