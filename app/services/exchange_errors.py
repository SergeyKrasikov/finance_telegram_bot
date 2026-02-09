def map_exchange_error(error_text: str) -> str:
    if "USDT rate is unknown" in error_text:
        return "Нужен курс USDT. Сначала обменяй USD↔USDT."
    if "Stablecoin rate is unknown" in error_text:
        return "Нет курса стейбла. Сначала обменяй стейбл → USD."
    if "Rates for" in error_text or "Rate for" in error_text:
        return "Нет курсов для выбранной пары. Сначала обменяй через USD."
    if "Exchange values must be greater than zero" in error_text:
        return "Суммы должны быть больше нуля."
    return "Не удалось выполнить обмен. Проверь формат и попробуй снова."
