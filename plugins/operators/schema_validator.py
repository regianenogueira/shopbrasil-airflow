from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos da FakeStore API antes do processamento.

    Recebe a lista de produtos (normalmente via XCom de outra task), confere os campos
    obrigatorios e seus tipos e devolve a mesma lista (que vira XCom da task).
    Dados fora do schema falham a task sem retry (AirflowFailException), pois
    retentar nao corrige um payload invalido.
    """

    template_fields = ("produtos",)

    CAMPOS_OBRIGATORIOS = {
        "id": int,
        "title": str,
        "price": (int, float),
        "category": str,
    }

    def __init__(self, produtos, **kwargs):
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context):
        if not isinstance(self.produtos, list) or not self.produtos:
            raise AirflowFailException("Lista de produtos vazia ou em formato invalido.")

        erros = []
        for i, produto in enumerate(self.produtos):
            if not isinstance(produto, dict):
                erros.append(f"item {i}: nao e um objeto")
                continue
            for campo, tipo in self.CAMPOS_OBRIGATORIOS.items():
                if campo not in produto:
                    erros.append(f"item {i}: campo '{campo}' ausente")
                elif not isinstance(produto[campo], tipo) or isinstance(produto[campo], bool):
                    erros.append(f"item {i}: campo '{campo}' com tipo invalido")
            if isinstance(produto.get("price"), (int, float)) and produto["price"] < 0:
                erros.append(f"item {i}: preco negativo")

        if erros:
            raise AirflowFailException(
                f"{len(erros)} problema(s) de schema: " + "; ".join(erros[:10])
            )

        self.log.info("%d produtos validados com sucesso.", len(self.produtos))
        return self.produtos
