import scrapy


FUNDS_EXPLORER_URL = "https://www.fundsexplorer.com.br/funds/{fundo}"

MESES = {
     "Janeiro": 1,
     "Fevereiro": 2,
     "Março": 3,
     "Abril": 4,
     "Maio": 5,
     "Junho": 6,
     "Julho": 7,
     "Agosto": 8,
     "Setembro": 9,
     "Outubro": 10,
     "Novembro": 11,
     "Dezembro": 12,
}


def parse_labels(labels):
    rows = []
    dia = "1"
    for label in labels:
        mes, ano = label.replace('"', "").split("/")
        rows.append(f"{ano}-{MESES[mes]}-{dia}")

    return rows


def parse_valores(valores):
    rows = []
    for valor in valores:
        rows.append(float(valor))

    return rows


class FIIs(scrapy.Spider):
    name = "fiis"
    start_urls = ["https://fiis.com.br/lista-de-fundos-imobiliarios/"]

    def parse(self, response):
        fundos = response.xpath("//span[@class='ticker']//text()")
        for fundo in fundos:
            cod = fundo.get()
            yield scrapy.Request(
                FUNDS_EXPLORER_URL.format(fundo=cod),
                meta={"cod_negociacao": cod},
                callback=self.parse_pagina_fii,
            )

    def parse_pagina_fii(self, response):
        cod_negociacao = response.meta.get("cod_negociacao")
        yield from self.parse_vp(response, cod_negociacao)
        yield from self.parse_dy(response, cod_negociacao)
        yield from self.parse_dividendos(response, cod_negociacao)

    def parse_chart(self, response, chart_id, value_name):
        js = response.xpath(
            f'//div[@id="{chart_id}"]//script/text()'
        ).extract_first()

        if js is None:
            return []

        labels = parse_labels(
            js.split('"labels":[')[1].split("]")[0].split(",")
        )
        values = parse_valores(
            js.split(f'"{value_name}","data":[')[1].split(']')[0].split(",")
        )

        return [{"data":d, "valor": v} for d, v in zip(labels, values)]

    def parse_dy(self, response, cod_negociacao):
        rows = self.parse_chart(
            response,
            chart_id="yields-chart-wrapper",
            value_name="Dividend Yield"
        )
        for row in rows:
            row.update({"tipo_info": "dy", "cod_negociacao": cod_negociacao})
            yield row

    def parse_vp(self, response, cod_negociacao):
        rows = self.parse_chart(
            response,
            chart_id="patrimonial-value-chart-wrapper",
            value_name="VP"
        )
        for row in rows:
            row.update({"tipo_info": "vp", "cod_negociacao": cod_negociacao})
            yield row

    def parse_dividendos(self, response, cod_negociacao):
        rows = self.parse_chart(
            response,
            chart_id="dividends-chart-wrapper",
            value_name="Dividendos"
        )
        for row in rows:
            row.update({"tipo_info": "dividendos", "cod_negociacao": cod_negociacao})
            yield row
