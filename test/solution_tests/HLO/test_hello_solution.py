from solutions.HLO.hello_solution import HelloSolution


class HelloSolution():
    def test_hello(self):
        assert HelloSolution().hello("Alice") == "Hello, Alice!"
        assert HelloSolution().hello("Bob") == "Hello, Bob!"


