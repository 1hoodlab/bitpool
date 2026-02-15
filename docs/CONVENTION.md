Based on the Clean Architecture and Domain-Driven Design (DDD) principles discussed in the sources, here is a professional **Project Convention** for your Actix-web project in Rust.

---

# 🦀 Actix-web Clean Architecture Convention

## 1. Core Philosophy
The primary goal of this architecture is **Separation of Concerns**. The system must be **independent of frameworks** (Actix), **independent of databases** (SQLx/Diesel), and **testable** without external elements.

### Key Rules:
*   **The Dependency Rule:** Dependencies must always point **inwards** toward the Domain Layer.
*   **Screaming Architecture:** The folder structure should reveal the business domain (e.g., `orders`, `users`), not the framework.
*   **Pure Core:** The Domain and Application layers must remain "pure" and should not import external libraries like Actix or database drivers.

---

## 2. Directory Structure
We follow a **Folder-by-Feature** approach within a layered architecture to ensure scalability.

```text
src/
├── domain/           # Level 1: Enterprise Business Rules (Pure Rust)
│   ├── entities/     # Domain objects with identity and behavior
│   └── repositories/ # Traits (Interfaces) for data access
├── application/      # Level 2: Application Business Rules
│   ├── use_cases/    # Orchestrators for specific tasks
│   └── dtos/         # Data structures for crossing boundaries
├── infrastructure/   # Level 3: Technical Details (Adapters)
│   ├── persistence/  # DB implementations (SQLx, Diesel, etc.)
│   └── external/     # External API clients (HTTP, Email)
├── presentation/     # Level 4: Entry Points
│   ├── handlers/     # Actix-web Route Handlers
│   └── main.rs       # Composition Root (Dependency Injection)
```

---

## 3. Layer Responsibilities

### 3.1 Domain Layer
*   **Entities:** Must not be "anemic." They should contain both data and the logic related to their state.
*   **Repository Traits:** Define **what** data is needed, not **how** it is fetched.

### 3.2 Application Layer
*   **Use Cases:** Each Use Case should perform **one single business action** (e.g., `CreateOrderUseCase`).
*   **DTOs:** Use plain structs to receive input and return output. Derive these from Entities where possible to avoid duplication.

### 3.3 Infrastructure Layer
*   **Adapters:** This layer implements the traits defined in the Domain. It is the only place where database-specific code (SQL) lives.

### 3.4 Presentation Layer (Actix)
*   **Handlers:** Their only job is to handle HTTP requests, validate input types, and delegate to a Use Case.
*   **Dependency Injection:** Use Actix’s `web::Data` to inject implementations into handlers at runtime.

---

## 4. Naming Conventions
To maintain consistency and readability, use the following functional suffixes:

| Type | Naming Pattern | Example |
| :--- | :--- | :--- |
| **Entity** | `[Name]` | `User`, `Order` |
| **Repository Trait** | `[Name]Repository` | `UserRepository` |
| **Use Case** | `[Action][Entity]UseCase` | `CreateUserUseCase` |
| **DTO** | `[Entity][Action][Input/Output]` | `UserCreateInput` |
| **File Name** | `[feature].[type].rs` | `user.repository.rs` |

---

## 5. Coding Standards for Rust
*   **Explicit Return Types:** Every function must have an explicit return type for readability and error detection.
*   **Abstraction over Implementation:** Use Cases must depend on **Traits**, never on concrete database structs.
*   **Error Handling:** Use custom Domain Errors (e.g., `InsufficientFundsError`) in the core. Map these to `HttpResponse` only in the Presentation layer.
*   **Async Traits:** Since Rust traits don't natively support async yet, use the `#[async_trait]` macro for repository definitions.

---

**Summary Strategy:** Start small with a simple modular structure and evolve toward this full Clean Architecture as your business logic becomes more complex.