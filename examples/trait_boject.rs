trait C {
    fn show(&self);
}

struct A {
    name: String,
    age: i32,
}

impl C for A {
    fn show(&self) {
        println!("name:{}, age{}", self.name, self.age)
    }
}

struct B {
    name: String,
    address: String,
}

impl C for B {
    fn show(&self) {
        println!("name:{}, age{}", self.name, self.address)
    }
}

fn main() {
    let a = A {
        name: "zhou".to_string(),
        age: 18,
    };

    let b = B {
        name: "上海".into(),
        address: "青浦区".into(),
    };
    let call1: &dyn C = &a;
    let call2: &dyn C = &b;

    call1.show();
    call2.show();
}
