class Vehiculo:
    def __init__(self, marca, modelo):
        self.marca = marca
        self.modelo = modelo

    def acelerar(self):
        print("Acelero")

    def frenar(self):
        print("Freno")


# Añadimos la clase de Vehiculo Acuatico
class VehiculoAcuatico(Vehiculo):
    def __init__(self, marca, model, flota):
        super().__init__(marca, model)
        self.flota = flota

    def navegar(self):
        print("Soy el vehiculo acutico y estoy navegando xd")

    def sumergir(self):
        print("Me estoy sumergiendo, ayuda")


# Añadimos la clase de Vehiculo Terrestre
class VehiculoTerrestre(Vehiculo):
    def __init__(self, marca, modelo, ruedas):
        super().__init__(marca, modelo)
        self.__ruedas = ruedas

    def get_ruedas(self):
        return self.__ruedas

    def conducir(self):
        print("Puedo moverme")

    def estacionar(self):
        print("Y tambien estacionarme")


# Añadimos la clase de Vehiculo Bus
class VehiculoBus(VehiculoTerrestre):
    def __init__(self, marca, modelo, ruedas, c_pasajeros):
        super().__init__(marca, modelo, ruedas)
        self.__c_pasajeros = c_pasajeros

    def get_c_pasajeros(self):
        return self.__c_pasajeros

    def abrir_puertas(self):
        print("Bus abriendo puertas.")

    def cerrar_puertas(self):
        print("Bus cerrando puertas.")


# Añadimos la clase de Vehiculo Tanque
class VehiculoTanque(VehiculoTerrestre):
    def __init__(self, marca, modelo, ruedas, armamento):
        super().__init__(marca, modelo, ruedas)
        self.__armamento = armamento

    def get_armamento(self):
        return self.__armamento

    def disparar(self):
        print("¡El tanque esta disparando! :c")

    def recargar_municion(self):
        print("¡Recargando!")


if __name__ == "__main__":
    v_generico = Vehiculo("Nissan", "Ferrari")
    v_generico.acelerar()
    v_generico.frenar()

    print("------------------------------------------------")

    barco = VehiculoAcuatico("Astondoa", "Ejecutivo", "Obvio que si floto jaja")
    barco.navegar()
    barco.sumergir()
    print(f"Marca: {barco.marca}, Modelo: {barco.modelo}, Flota: {barco.flota}")

    print("------------------------------------------------")

    auto = VehiculoTerrestre("Tesla", "Model S", 4)
    auto.conducir()
    auto.estacionar()
    print(f"Marca: {auto.marca}, Modelo: {auto.modelo}, Ruedas: {auto.get_ruedas()}")

    print("------------------------------------------------")

    bus = VehiculoBus("Primera Plus", "Gallinero", 6, 30)
    bus.conducir()
    bus.estacionar()
    bus.abrir_puertas()
    print(
        f"Marca: {bus.marca}, Modelo: {bus.modelo}, Ruedas: {bus.get_ruedas()}, Capacidad de pasajeros: {bus.get_c_pasajeros()}")

    print("------------------------------------------------")

    tanque = VehiculoTanque("M-95 Degman", "M-95", 8, "Balas de salva")
    tanque.conducir()
    tanque.estacionar()
    tanque.disparar()
    print(
        f"Marca: {tanque.marca}, Modelo: {tanque.modelo}, Ruedas: {tanque.get_ruedas()}, Armamento: {tanque.get_armamento()}")
    print("------------------------------------------------")
