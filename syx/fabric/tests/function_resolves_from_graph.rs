//! A `Function` and everything it references resolve back out of a
//! `fabric::Repository`, using only its own digest.

mod common;
use common::temp_graph;

#[tokio::test]
async fn action_variant_and_its_command_and_config_resolve_from_graph() {
    let (_dir, graph) = temp_graph().await;

    // The command to run, once, directly.
    let command = fabric::Command::new("python3").arg("main.py");
    let command_digest = graph.put(&command).await.unwrap();

    // The config: a Tree with one file entry, itself resolvable from
    // the graph.
    let file_digest = graph.put(&cas::Bytes::from_static(b"threshold: 10")).await.unwrap();
    let config =
        fabric::Tree::new([("config.yaml".to_string(), fabric::Node::Blob(file_digest))], []);
    let config_digest = graph.put(&config).await.unwrap();

    let function = fabric::Function::action(command_digest, config_digest);
    let function_digest = graph.put(&function).await.unwrap();

    // Read the whole graph back out of the graph using only the
    // function's digest. Input isn't part of this graph at all: it's
    // supplied separately, at call time, by whoever runs this.
    let resolved_function: fabric::Function = graph.get(&function_digest).await.unwrap().unwrap();
    assert_eq!(resolved_function, function);

    let (resolved_command_digest, resolved_config_digest) = match resolved_function {
        fabric::Function::Action { command, config } => (command, config),
        _ => panic!("expected Action"),
    };

    let resolved_command: fabric::Command =
        graph.get(&resolved_command_digest).await.unwrap().unwrap();
    assert_eq!(resolved_command, command);

    let resolved_config: fabric::Tree = graph.get(&resolved_config_digest).await.unwrap().unwrap();
    assert_eq!(resolved_config, config);

    // The file the config tree references is itself resolvable.
    assert_eq!(
        graph.get(&file_digest).await.unwrap(),
        Some(cas::Bytes::from_static(b"threshold: 10"))
    );
}

#[tokio::test]
async fn server_variant_and_its_command_and_config_resolve_from_graph() {
    let (_dir, graph) = temp_graph().await;

    // The command to run as the persistent process.
    let command = fabric::Command::new("serve").arg("--config");
    let command_digest = graph.put(&command).await.unwrap();

    // The config: a Tree with one file entry, itself resolvable from
    // the graph.
    let file_digest = graph.put(&cas::Bytes::from_static(b"port: 8080")).await.unwrap();
    let config =
        fabric::Tree::new([("config.yaml".to_string(), fabric::Node::Blob(file_digest))], []);
    let config_digest = graph.put(&config).await.unwrap();

    // The function tying command and config together, callable as a server.
    let function = fabric::Function::server(command_digest, config_digest);
    let function_digest = graph.put(&function).await.unwrap();

    // Read the whole graph back out of the graph using only the
    // function's digest -- the resolution a caller would do before
    // invoking it.
    let resolved_function: fabric::Function = graph.get(&function_digest).await.unwrap().unwrap();
    assert_eq!(resolved_function, function);

    let (resolved_command_digest, resolved_config_digest) = match resolved_function {
        fabric::Function::Server { command, config } => (command, config),
        _ => panic!("expected Server"),
    };

    let resolved_command: fabric::Command =
        graph.get(&resolved_command_digest).await.unwrap().unwrap();
    assert_eq!(resolved_command, command);

    let resolved_config: fabric::Tree = graph.get(&resolved_config_digest).await.unwrap().unwrap();
    assert_eq!(resolved_config, config);

    // The file the config tree references is itself resolvable.
    assert_eq!(
        graph.get(&file_digest).await.unwrap(),
        Some(cas::Bytes::from_static(b"port: 8080"))
    );
}
