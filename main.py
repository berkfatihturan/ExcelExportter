import importlib
from config import VERSION

def run_versioned_code():
    try:
        module_path = f"{VERSION}.runner"  # Örn: v06.runner
        module = importlib.import_module(module_path)
        if hasattr(module, "run"):
            module.run()
        else:
            raise AttributeError(f"'{module_path}' içinde 'run' fonksiyonu bulunamadı.")
    except Exception as e:
        print(f"[!] Hata: {e}")

if __name__ == "__main__":
    run_versioned_code()