# 🤝 Contributing to CoMaKo

Thank you for your interest in **CoMaKo – the Community Market Communication Platform**!  
This project is made for and by people who believe that **electricity should be local, fair, and in our hands**.

Whether you're a developer, a solar panel owner, a retiree with admin skills, or a curious rebel — you are welcome here. 🙌

---

## 💡 What You Can Contribute

### 🛠 Developers
- Help build or improve REST APIs (`FastAPI`, `Flask`, or similar)
- Write EDI parsers and converters for UTILMD / MSCONS / INVOIC
- Implement new agents (e.g. forecast bot, tariff optimizer)
- Improve CLI tools or add a basic front-end
- Write tests or refactor code for stability

### 🧓 Community Operators
- Test the system with real or demo data
- Help document how to run CoMaKo in your neighborhood or coop
- Propose features that help real people use this in daily life
- Translate README and UI for non-tech users

### ✍️ Writers / Educators
- Create simple step-by-step guides
- Write blog posts or explainers on local energy sharing
- Record walkthroughs or voicebot tutorials

---

## 🧑‍💻 How to Get Started

1. **Fork the repository**
2. Create a branch:  
   ~~~bash
   git checkout -b your-feature-name
   ~~~
3. Make your changes
4. Push to your fork and open a **Pull Request**
5. Include a clear description of **what you changed and why**

---

## 🔍 Code Guidelines

- Use English for code and comments
- Stick to PEP8 (for Python) or equivalent style for your language
- Write modular, testable functions
- Use meaningful commit messages

---

## 📂 File Structure Overview (simplified)

```
├── market_core/        # Core logic: balance groups, billing, storage
├── edi_gateway/        # EDI message parsing & simulation
├── meter_gateway/      # APIs for user/device input (e.g. voicebot)
├── data/               # Sample data & EDI examples
├── tests/              # Test cases (unit/integration)
├── README.md
├── CONTRIBUTING.md
└── LICENSE
```

---

## 🧪 Test It Locally

Coming soon:  
A `docker-compose.yml` for easy local testing with all three services.

If you're contributing before that, feel free to just run the modules individually or with dummy data.

---

## 🗣 Code of Conduct

Be kind, clear, constructive.  
We welcome all contributors regardless of age, background, or energy provider.  
This is a place for builders, not egos.

---

## 👐 License & Ownership

This project is licensed under **MIT**.  
It belongs to everyone. Use it, remix it, and make local energy happen.

---

Thanks for helping us bring **local energy freedom** to life. ⚡

> _From rooftops to rebels – this is your grid now._

