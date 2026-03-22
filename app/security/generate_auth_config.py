import json,hashlib,getpass,sys
from pathlib import Path

SEC_DIR=Path(__file__).resolve().parent
TPL=SEC_DIR/"auth_config.template.json"
OUT=SEC_DIR/"auth_config.json"

def sha256_hex(x): return hashlib.sha256(x.encode("utf-8")).hexdigest()

def main():
    if not TPL.exists():
        print(f"Template introuvable: {TPL}")
        sys.exit(1)
    with open(TPL,"r",encoding="utf-8") as f:
        cfg=json.load(f)
    username=input(f'Username [{cfg.get("username","admin")}]: ').strip() or str(cfg.get("username","admin"))
    pw=getpass.getpass("Nouveau mot de passe local: ").strip()
    pw2=getpass.getpass("Confirme le mot de passe: ").strip()
    if not pw:
        print("Mot de passe vide interdit.")
        sys.exit(1)
    if pw!=pw2:
        print("Les mots de passe ne correspondent pas.")
        sys.exit(1)
    salt=str(cfg.get("password_salt","")).strip()
    if not salt:
        print("Salt manquant dans le template.")
        sys.exit(1)
    cfg["username"]=username
    cfg["password_hash_sha256_of_salt_double_colon_password"]=sha256_hex(salt+"::"+pw)
    cfg["password_placeholder_plaintext_for_setup_only"]="CONFIGURED_LOCALLY"
    with open(OUT,"w",encoding="utf-8") as f:
        json.dump(cfg,f,indent=2)
    print(f"Fichier généré: {OUT}")

if __name__=="__main__":
    main()
