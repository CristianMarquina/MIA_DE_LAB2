def format_time(t):
    """
    Formats an integer time (e.g., 905) into a string ('09:05:00').
    Pads with leading zeros to ensure 4 digits.
    """
    t_str = str(int(t)).zfill(4)
    return f"{t_str[:2]}:{t_str[2:]}:00"