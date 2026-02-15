#[test]
fn tracing_feature_gating_compiles() {
    #[cfg(feature = "tracing")]
    {
        tracing::event!(
            tracing::Level::DEBUG,
            event = "ragpipe.test.feature_gating",
            "ragpipe.test.feature_gating"
        );
    }

    #[cfg(not(feature = "tracing"))]
    {
        let marker = "tracing-disabled";
        assert_eq!(marker, "tracing-disabled");
    }
}
