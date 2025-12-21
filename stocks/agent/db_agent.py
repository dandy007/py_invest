import os
import sys
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI

# 1. Basic paths
AGENT_DIR = Path(__file__).parent.resolve()
CONFIG_FILE = AGENT_DIR / "config.yaml"
CONVERSATIONS_FILE = AGENT_DIR / "conversations.json"
STOCKS_DIR = AGENT_DIR.parent
ROOT_DIR = STOCKS_DIR.parent

# 2. Add root to path for imports to work correctly
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# 3. Load .env (checking multiple locations)
env_paths = [
    ROOT_DIR / ".env",
    STOCKS_DIR / ".env",
    AGENT_DIR / ".env",
    Path(".env").resolve()
]

for env_path in env_paths:
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        break
else:
    load_dotenv() # Fallback

# 4. App imports
from stocks.agent.db_tools import TOOLS, DBTools


def load_config() -> dict:
    """Load configuration from YAML file."""
    import yaml
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {
        "model": "google/gemini-2.0-flash-001",
        "personalities": {
            "default": {
                "name": "Analyst",
                "system_prompt": "Jsi profesionální finanční analytik. Odpovídáš věcně a stručně. Používáš data z databáze k podpoře svých tvrzení."
            }
        },
        "active_personality": "default"
    }


def save_config(config: dict):
    """Save configuration to YAML file."""
    import yaml
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


def load_conversations() -> dict:
    """Load conversations from JSON file."""
    if CONVERSATIONS_FILE.exists():
        with open(CONVERSATIONS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"conversations": {}, "active_id": None}


def save_conversations(data: dict):
    """Save conversations to JSON file."""
    with open(CONVERSATIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


class DBAgent:
    """Interactive database analysis agent."""
    
    def __init__(self):
        self.config = load_config()
        self.conv_data = load_conversations()
        self.db_tools = DBTools()
        
        # Initialize OpenAI client for OpenRouter
        api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
        # Remove common wrapping quotes if present
        if api_key.startswith('"') and api_key.endswith('"'):
            api_key = api_key[1:-1]
        elif api_key.startswith("'") and api_key.endswith("'"):
            api_key = api_key[1:-1]
            
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY not found in environment variables")
        
        # Diagnostic print (safe)
        key_show = f"{api_key[:6]}...{api_key[-4:]}" if len(api_key) > 10 else "***"
        print(f"  🔑 API Key loaded: {key_show} (length: {len(api_key)})")

        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
            default_headers={
                "HTTP-Referer": "https://github.com/dandy007/py_invest", # Optional, for OpenRouter rankings
                "X-Title": "PyInvest DB Agent", # Optional, for OpenRouter rankings
            }
        )
        
        # Current conversation
        self.current_conv_id = self.conv_data.get("active_id")
        self.messages = []
        
        if self.current_conv_id and self.current_conv_id in self.conv_data["conversations"]:
            self.messages = self.conv_data["conversations"][self.current_conv_id]["messages"]
        else:
            self._new_conversation()
    
    def _get_system_prompt(self) -> str:
        """Get current personality's system prompt."""
        personality_name = self.config.get("active_personality", "default")
        personalities = self.config.get("personalities", {})
        personality = personalities.get(personality_name, personalities.get("default", {}))
        
        base_prompt = personality.get("system_prompt", "Jsi finanční analytik.")
        
        return f"""{base_prompt}

Máš přístup k databázi s investičními daty. Můžeš používat nástroje pro dotazování databáze.
Tabulka 'tickers' obsahuje základní informace o akciích (price, PE, PS, PB, growth_rate, market_cap, atd.)
Tabulka 'tickers_time_data' obsahuje historická data (revenue, earnings, cash flow, margins, atd.)

Když uživatel požádá o analýzu, použij dostupné nástroje k získání dat a poté je interpretuj. Odpovídej v češtině pokud uživatel používá češtinu."""
    
    def _new_conversation(self, title: str = None):
        """Start a new conversation."""
        self.current_conv_id = str(uuid.uuid4())[:8]
        self.messages = []
        
        if title is None:
            title = f"Konverzace {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        self.conv_data["conversations"][self.current_conv_id] = {
            "title": title,
            "created": datetime.now().isoformat(),
            "messages": []
        }
        self.conv_data["active_id"] = self.current_conv_id
        save_conversations(self.conv_data)
        
        print(f"\n✨ Nová konverzace: {title} (ID: {self.current_conv_id})")
    
    def _save_current_conversation(self):
        """Save current conversation state."""
        if self.current_conv_id:
            self.conv_data["conversations"][self.current_conv_id]["messages"] = self.messages
            self.conv_data["active_id"] = self.current_conv_id
            save_conversations(self.conv_data)
    
    def _handle_command(self, command: str) -> bool:
        """Handle slash commands. Returns True if should continue, False to exit."""
        parts = command.strip().split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else None
        
        if cmd == "/exit" or cmd == "/quit":
            print("\n👋 Nashledanou!")
            return False
        
        elif cmd == "/new":
            self._new_conversation(arg)
        
        elif cmd == "/list":
            print("\n📋 Konverzace:")
            for cid, conv in self.conv_data["conversations"].items():
                marker = "➤ " if cid == self.current_conv_id else "  "
                print(f"{marker}{cid}: {conv['title']}")
        
        elif cmd == "/switch":
            if arg and arg in self.conv_data["conversations"]:
                self._save_current_conversation()
                self.current_conv_id = arg
                self.messages = self.conv_data["conversations"][arg]["messages"]
                print(f"\n🔄 Přepnuto na: {self.conv_data['conversations'][arg]['title']}")
            else:
                print(f"\n❌ Konverzace '{arg}' neexistuje")
        
        elif cmd == "/personality":
            if arg is None:
                print("\n🎭 Dostupné personality:")
                for name, p in self.config.get("personalities", {}).items():
                    marker = "➤ " if name == self.config.get("active_personality") else "  "
                    print(f"{marker}{name}: {p.get('name', name)}")
            elif arg == "add":
                self._add_personality()
            elif arg in self.config.get("personalities", {}):
                self.config["active_personality"] = arg
                save_config(self.config)
                print(f"\n🎭 Personalita změněna na: {arg}")
            else:
                print(f"\n❌ Personalita '{arg}' neexistuje")
        
        elif cmd == "/model":
            if arg:
                self.config["model"] = arg
                save_config(self.config)
                print(f"\n🤖 Model změněn na: {arg}")
            else:
                print(f"\n🤖 Aktuální model: {self.config.get('model')}")
        
        elif cmd == "/help":
            print("""
📖 Dostupné příkazy:
  /new [název]     - Nová konverzace
  /list            - Seznam konverzací
  /switch <id>     - Přepnout konverzaci
  /personality     - Zobrazit/změnit personalitu
  /model [název]   - Zobrazit/změnit model
  /help            - Tato nápověda
  /exit            - Ukončit
            """)
        
        else:
            print(f"\n❓ Neznámý příkaz: {cmd}. Napište /help pro nápovědu.")
        
        return True
    
    def _add_personality(self):
        """Interactively add a new personality."""
        print("\n🎭 Přidání nové personality:")
        name = input("  Název (klíč): ").strip()
        if not name:
            print("  ❌ Název je povinný")
            return
        
        display_name = input("  Zobrazovaný název: ").strip() or name
        print("  System prompt (ukončete prázdným řádkem):")
        
        lines = []
        while True:
            line = input("  ")
            if not line:
                break
            lines.append(line)
        
        prompt = "\n".join(lines)
        
        if "personalities" not in self.config:
            self.config["personalities"] = {}
        
        self.config["personalities"][name] = {
            "name": display_name,
            "system_prompt": prompt
        }
        save_config(self.config)
        print(f"\n✅ Personalita '{name}' přidána")
    
    def _call_llm(self, user_message: str) -> str:
        """Call LLM with tool support."""
        # Add user message
        self.messages.append({"role": "user", "content": user_message})
        
        # Prepare messages with system prompt
        messages = [{"role": "system", "content": self._get_system_prompt()}] + self.messages
        
        try:
            # First call
            response = self.client.chat.completions.create(
                model=self.config.get("model", "google/gemini-2.0-flash-001"),
                messages=messages,
                tools=TOOLS,
                tool_choice="auto"
            )
            
            assistant_message = response.choices[0].message
            
            # Handle tool calls
            while assistant_message.tool_calls:
                # Add assistant message with tool calls
                self.messages.append({
                    "role": "assistant",
                    "content": assistant_message.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments
                            }
                        }
                        for tc in assistant_message.tool_calls
                    ]
                })
                
                # Execute tools and add results
                for tool_call in assistant_message.tool_calls:
                    func_name = tool_call.function.name
                    func_args = json.loads(tool_call.function.arguments)
                    
                    print(f"  📊 Volám: {func_name}...")
                    result = self.db_tools.execute_tool(func_name, func_args)
                    
                    self.messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": result
                    })
                
                # Continue conversation
                messages = [{"role": "system", "content": self._get_system_prompt()}] + self.messages
                response = self.client.chat.completions.create(
                    model=self.config.get("model", "google/gemini-2.0-flash-001"),
                    messages=messages,
                    tools=TOOLS,
                    tool_choice="auto"
                )
                assistant_message = response.choices[0].message
            
            # Final response
            final_content = assistant_message.content or ""
            self.messages.append({"role": "assistant", "content": final_content})
            self._save_current_conversation()
            
            return final_content
            
        except Exception as e:
            error_msg = f"❌ Chyba: {str(e)}"
            print(error_msg)
            return error_msg
    
    def run(self):
        """Main interaction loop."""
        print("""
╔══════════════════════════════════════════════════════════════╗
║  🤖 DB Analysis Agent                                        ║
║  Interaktivní agent pro analýzu investičních dat             ║
║  Napište /help pro seznam příkazů                            ║
╚══════════════════════════════════════════════════════════════╝
        """)
        
        personality = self.config.get("active_personality", "default")
        model = self.config.get("model", "unknown")
        print(f"  Model: {model}")
        print(f"  Personalita: {personality}")
        print(f"  Konverzace: {self.current_conv_id}")
        print("━" * 60)
        
        try:
            while True:
                try:
                    user_input = input("\n> ").strip()
                except EOFError:
                    break
                
                if not user_input:
                    continue
                
                # Handle commands
                if user_input.startswith("/"):
                    if not self._handle_command(user_input):
                        break
                    continue
                
                # Call LLM
                print()
                response = self._call_llm(user_input)
                print(f"\n{response}")
        
        finally:
            self.db_tools.close()
            self._save_current_conversation()


def main():
    """Entry point."""
    try:
        agent = DBAgent()
        agent.run()
    except ValueError as e:
        print(f"❌ Chyba konfigurace: {e}")
        print("Přidejte OPENROUTER_API_KEY do .env souboru")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n👋 Ukončeno uživatelem")
        sys.exit(0)


if __name__ == "__main__":
    main()
